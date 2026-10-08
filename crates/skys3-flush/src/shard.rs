//! The flusher of one shard (§7.1): which keys are dirty, which are being
//! flushed, which are held in conflict, and the task that drives them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::ConflictPolicy;
use skys3_index::{Entry, EntryState};
use skys3_io::{Disk, WallClock};
use skys3_log::record::Flushed;
use skys3_log::{RecordBody, ShardRef};
use skys3_remote::ObjectStore;
use skys3_shard::{Change, FlushState, FlushWaiter, Shard, ShardError};
use skys3_types::{ETag, EpochSeq, Seq};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::attempt::{Attempt, Conflict, Outcome, Remote};
use crate::budget::Charge;
use crate::conflict::{ConflictBug, Unresolved, conflict_bug};
use crate::peer::hooks as peer_hooks;
use crate::peer::{Bodies, BodyStep, FlushedRecord, PeerBug};
use crate::stream::{Step, Streams};
use crate::target::Target;

/// The target a bucket's flushers send to, once its capability probe is
/// done.
///
/// Not a `watch` channel: a `watch` wakes the receivers waiting on it in
/// an order it draws at random, so the shard flushers of a bucket would
/// start flushing in an order that no simulation seed decides. A `Notify`
/// wakes them in the order they began to wait.
#[derive(Debug)]
pub(crate) struct Ready<S> {
    target: Mutex<Option<Arc<Target<S>>>>,
    set: Notify,
}

impl<S> Ready<S> {
    /// A cell holding `target`.
    pub(crate) fn new(target: Option<Arc<Target<S>>>) -> Arc<Self> {
        Arc::new(Self {
            target: Mutex::new(target),
            set: Notify::new(),
        })
    }

    /// The target, if the probe is done.
    pub(crate) fn get(&self) -> Option<Arc<Target<S>>> {
        self.target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Makes `target` the target, and wakes every flusher waiting for it.
    pub(crate) fn set(&self, target: Arc<Target<S>>) {
        *self.target.lock().unwrap_or_else(PoisonError::into_inner) = Some(target);
        self.set.notify_waiters();
    }
}

/// How many entries the startup scan reads per index transaction.
const SCAN_PAGE: usize = 512;

/// Where a dirty key is in its flush (§4.2). `Clean` has no phase: a clean
/// key is not tracked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    /// Dirty: waiting for a flush slot, in line by when it became dirty.
    Dirty,
    /// Dirty, and waiting until `Instant` to retry after a failure.
    Backoff(Instant),
    /// The flusher has taken its latest version.
    Flushing,
    /// A flush found the remote changed out of band; the key is held
    /// (§7.2).
    Conflict(Conflict),
}

/// A dirty key the flusher tracks.
#[derive(Debug, Clone)]
struct Tracked {
    /// Its place in line: the position of the first version not yet at the
    /// remote. Keys are flushed oldest first, in `seq` order (§7.1).
    order: EpochSeq,
    /// When its oldest change not yet at the remote was made, on the wall
    /// clock.
    since: Duration,
    /// The newest version seen.
    latest: EpochSeq,
    /// The newest version's size, 0 for a tombstone.
    size: u64,
    phase: Phase,
    /// Failed attempts since the last success.
    failures: u32,
    /// When a change arrived while the key was flushing.
    newer_since: Option<Duration>,
    /// The newest version the remote is known to hold while the key is
    /// still tracked: a tombstone the import has not passed, or a version
    /// flushed while a newer one committed.
    remote: Option<EpochSeq>,
    /// Write-through writes waiting for versions not yet at the remote
    /// (§7.5).
    waiters: Vec<FlushWaiter>,
    /// How its conflict was resolved, until a flush of the key succeeds
    /// (§7.2).
    resolution: Option<ConflictPolicy>,
}

/// One held conflict, as the admin API lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictStatus {
    /// The key.
    pub key: String,
    /// The local version that could not be flushed.
    pub seq: Seq,
    /// The remote's ETag, if it has an object.
    pub remote_etag: Option<ETag>,
    /// The remote object's write identity, if it has one.
    pub remote_identity: Option<String>,
}

/// A shard flusher's state at one moment.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ShardStatus {
    /// Keys whose latest change is not at the remote, excluding conflicts.
    pub dirty: u64,
    /// Of those, the keys being flushed.
    pub flushing: u64,
    /// Bytes of the versions not at the remote, every key included.
    pub dirty_bytes: u64,
    /// When the oldest change not at the remote was made, every key
    /// included, on the wall clock.
    pub oldest_dirty: Option<Duration>,
    /// When the oldest change the flusher is still working on was made:
    /// conflicts excluded.
    pub oldest_pending: Option<Duration>,
    /// Keys held in conflict.
    pub conflicts: Vec<ConflictStatus>,
    /// The latest error a flush attempt hit, as `key: error`, among the
    /// keys whose flush has not got past its error since: a key's error is
    /// cleared once a later attempt flushes or settles the key, or finds it
    /// in conflict.
    pub last_error: Option<String>,
    /// Remote multipart uploads streamed for the shard's uploads (§7.3)
    /// that are open, not completed or aborted yet; for a native target,
    /// the streamed bodies being staged (§7.8).
    pub streams: u64,
    /// Whether the flusher stopped, because its shard did.
    pub stopped: bool,
}

/// What the flusher knows, shared between its task and status readers.
#[derive(Debug, Default)]
struct State {
    keys: HashMap<String, Tracked>,
    /// Dirty keys by their place in line.
    ready: BTreeMap<EpochSeq, String>,
    /// Keys backing off, by when they may retry.
    backoff: BTreeMap<(Instant, EpochSeq), String>,
    /// Keys not in conflict, by when their oldest pending change was made.
    pending: BTreeSet<(Duration, String)>,
    /// What flushes put at the remote while their `FLUSHED` records are
    /// not yet applied, by key.
    recording: HashMap<String, Remote>,
    dirty_bytes: u64,
    flushing: u64,
    /// The latest unresolved error of each key, numbered by when it
    /// happened.
    errors: HashMap<String, (u64, String)>,
    /// The number of the next error.
    next_error: u64,
    stopped: bool,
    /// The budget `dirty_bytes` counts against, until the flusher stops.
    charge: Option<Charge>,
}

impl State {
    /// Tracks a change to `key`: a new version at `position` of `size`
    /// bytes (`None` keeps the size), first seen at `now`.
    fn changed(&mut self, key: &str, position: EpochSeq, size: Option<u64>, since: Duration) {
        let Some(tracked) = self.keys.get_mut(key) else {
            let size = size.unwrap_or(0);
            self.set_dirty_bytes(self.dirty_bytes + size);
            self.ready.insert(position, key.to_owned());
            self.pending.insert((since, key.to_owned()));
            let tracked = Tracked {
                order: position,
                since,
                latest: position,
                size,
                phase: Phase::Dirty,
                failures: 0,
                newer_since: None,
                remote: None,
                waiters: Vec::new(),
                resolution: None,
            };
            self.keys.insert(key.to_owned(), tracked);
            return;
        };
        if position <= tracked.latest {
            return;
        }
        tracked.latest = position;
        if tracked.phase == Phase::Flushing {
            tracked.newer_since.get_or_insert(since);
        }
        if let Some(size) = size {
            let old = std::mem::replace(&mut tracked.size, size);
            self.set_dirty_bytes(self.dirty_bytes - old + size);
        }
    }

    /// Sets the dirty bytes, and moves the budget's count with them.
    fn set_dirty_bytes(&mut self, bytes: u64) {
        if let Some(charge) = &self.charge {
            charge.adjust(self.dirty_bytes, bytes);
        }
        self.dirty_bytes = bytes;
    }

    /// Marks the flusher stopped: its dirty bytes no longer count against
    /// the budget, since a new flusher of the shard counts them again.
    fn stop(&mut self) {
        self.stopped = true;
        // Unanswered, a wait ends, and its writer asks the flusher started
        // next, here or on a new primary. The waits end in key order, not
        // the map's, so a simulation seed replays exactly.
        let mut keys: Vec<_> = self.keys.iter_mut().collect();
        keys.sort_unstable_by_key(|(key, _)| *key);
        for (_, tracked) in keys {
            tracked.waiters.clear();
        }
        if let Some(charge) = self.charge.take() {
            charge.adjust(self.dirty_bytes, 0);
        }
    }

    /// Takes the next dirty key for a flush, if any, with how its conflict
    /// was resolved.
    fn take_ready(&mut self) -> Option<(String, EpochSeq, Option<ConflictPolicy>)> {
        let (_, key) = self.ready.pop_first()?;
        let tracked = self.keys.get_mut(&key)?;
        tracked.phase = Phase::Flushing;
        tracked.newer_since = None;
        self.flushing += 1;
        Some((key, tracked.latest, tracked.resolution))
    }

    /// Moves keys whose backoff ended back in line.
    fn release_due(&mut self, now: Instant) {
        while let Some(entry) = self.backoff.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let key = entry.remove();
            if let Some(tracked) = self.keys.get_mut(&key) {
                tracked.phase = Phase::Dirty;
                self.ready.insert(tracked.order, key);
            }
        }
    }

    /// When the next backoff ends.
    fn next_release(&self) -> Option<Instant> {
        self.backoff.keys().next().map(|(at, _)| *at)
    }

    /// Ends the flush of `key` that started when its newest version was
    /// `dispatched`: the key is clean unless a newer version arrived, in
    /// which case it is dirty again from that version on.
    fn finished(&mut self, key: &str, dispatched: EpochSeq) {
        self.flushing -= 1;
        self.errors.remove(key);
        let Some(tracked) = self.keys.get_mut(key) else {
            return;
        };
        tracked.failures = 0;
        // A resolution ends with the flush that carried it out: a newer
        // version builds on what that flush left at the remote.
        tracked.resolution = None;
        match tracked.newer_since.take() {
            Some(since) if tracked.latest > dispatched => {
                self.pending.remove(&(tracked.since, key.to_owned()));
                tracked.order = tracked.latest;
                tracked.since = since;
                tracked.phase = Phase::Dirty;
                self.pending.insert((since, key.to_owned()));
                self.ready.insert(tracked.order, key.to_owned());
            }
            _ => self.untrack(key),
        }
    }

    /// Counts another attempt of `key` that must wait, and returns how many
    /// in a row there were.
    fn wait(&mut self, key: &str) -> u32 {
        self.keys.get_mut(key).map_or(1, |tracked| {
            tracked.failures += 1;
            tracked.failures
        })
    }

    /// Ends the flush of `key` with a failure: it retries at `at`.
    fn failed(&mut self, key: &str, error: String, at: Instant) {
        self.flushing -= 1;
        self.record_error(key, error);
        if let Some(tracked) = self.keys.get_mut(key) {
            tracked.phase = Phase::Backoff(at);
            self.backoff.insert((at, tracked.order), key.to_owned());
        }
    }

    /// Ends the flush of `key` in conflict, which `policy` decides (§7.2):
    /// under `hold` the key is held, and no longer in line; under the
    /// other policies the conflict is resolved at once, and the key is
    /// dirty again.
    fn conflicted(&mut self, key: &str, conflict: Conflict, policy: ConflictPolicy) {
        self.flushing -= 1;
        self.errors.remove(key);
        let Some(tracked) = self.keys.get_mut(key) else {
            return;
        };
        if policy == ConflictPolicy::Hold {
            self.pending.remove(&(tracked.since, key.to_owned()));
            tracked.phase = Phase::Conflict(conflict);
            for waiter in tracked.waiters.drain(..) {
                waiter.answer(FlushState::Conflict);
            }
            return;
        }
        tracked.phase = Phase::Dirty;
        self.ready.insert(tracked.order, key.to_owned());
        Self::resolved(tracked, policy);
    }

    /// Resolves the conflict `key` is held in under `policy`, as an
    /// operator asks: the key is dirty again, in line, and its next flush
    /// carries out the resolution (§4.2 Conflict → Dirty).
    fn resolve(&mut self, key: &str, policy: ConflictPolicy) -> Result<(), Unresolved> {
        let tracked = self.keys.get_mut(key).ok_or(Unresolved::NotHeld)?;
        if !matches!(tracked.phase, Phase::Conflict(_)) {
            return Err(Unresolved::NotHeld);
        }
        tracked.phase = Phase::Dirty;
        tracked.failures = 0;
        self.pending.insert((tracked.since, key.to_owned()));
        self.ready.insert(tracked.order, key.to_owned());
        Self::resolved(tracked, policy);
        Ok(())
    }

    /// Records that `tracked`'s conflict is resolved under `policy`. A
    /// write-through write waiting for a version that `discard_local` will
    /// drop learns of the conflict now; one that `overwrite` will flush
    /// keeps waiting for it.
    fn resolved(tracked: &mut Tracked, policy: ConflictPolicy) {
        tracked.resolution = Some(policy);
        if policy == ConflictPolicy::DiscardLocal {
            for waiter in tracked.waiters.drain(..) {
                waiter.answer(FlushState::Conflict);
            }
        }
    }

    /// Records that `discard_local` dropped `version` of `key` for the
    /// remote's write: the writes waiting for it, or an older version,
    /// learn of the conflict.
    fn discarded(&mut self, key: &str, version: EpochSeq) {
        if let Some(tracked) = self.keys.get_mut(key) {
            tracked.waiters.retain(|waiter| {
                let dropped = waiter.version() <= version;
                if dropped {
                    waiter.answer(FlushState::Conflict);
                }
                !dropped && waiter.is_waited()
            });
        }
    }

    /// Stops tracking the held `key` as if its version had been flushed,
    /// and returns its `seq` and the conflict: the seeded bug
    /// [`ConflictBug::ResolvesClean`].
    fn resolve_clean(&mut self, key: &str) -> Result<(Seq, Conflict), Unresolved> {
        let tracked = self.keys.get(key).ok_or(Unresolved::NotHeld)?;
        let Phase::Conflict(conflict) = &tracked.phase else {
            return Err(Unresolved::NotHeld);
        };
        let resolved = (tracked.latest.seq, conflict.clone());
        self.untrack(key);
        Ok(resolved)
    }

    /// Records that the remote holds `version` of `key`, or a later one,
    /// and answers the waits it covers.
    fn reached(&mut self, key: &str, version: EpochSeq) {
        if let Some(tracked) = self.keys.get_mut(key) {
            tracked.remote = tracked.remote.max(Some(version));
        }
        self.answer_waits(key, version);
    }

    /// Answers the write-through waits of `key` that `version` covers.
    fn answer_waits(&mut self, key: &str, version: EpochSeq) {
        if let Some(tracked) = self.keys.get_mut(key) {
            tracked.waiters.retain(|waiter| {
                let covered = waiter.version() <= version;
                if covered {
                    waiter.answer(FlushState::Flushed);
                }
                !covered && waiter.is_waited()
            });
        }
    }

    /// Takes a write-through write's wait for its version (§7.5). The
    /// shard passes it after the change that stored the version, so an
    /// untracked key has had its latest version, at or after the one
    /// waited for, flushed.
    fn awaited(&mut self, waiter: FlushWaiter) {
        let Some(tracked) = self.keys.get_mut(waiter.key()) else {
            return waiter.answer(FlushState::Flushed);
        };
        if matches!(tracked.phase, Phase::Conflict(_)) {
            waiter.answer(FlushState::Conflict);
        } else if tracked.remote >= Some(waiter.version()) {
            waiter.answer(FlushState::Flushed);
        } else {
            tracked.waiters.retain(FlushWaiter::is_waited);
            tracked.waiters.push(waiter);
        }
    }

    /// Records `error` as `key`'s latest.
    fn record_error(&mut self, key: &str, error: String) {
        self.errors.insert(key.to_owned(), (self.next_error, error));
        self.next_error += 1;
    }

    /// Whether a flush of `key` is still to come: it is dirty, and not
    /// held in conflict. Otherwise the remote uploads streamed for its
    /// closed uploads will never complete (§7.3): a clean key holds its
    /// latest version at the remote, and a held one is flushed only once
    /// its conflict is resolved, by a flush after commit.
    fn awaits_flush(&self, key: &str) -> bool {
        self.keys
            .get(key)
            .is_some_and(|tracked| !matches!(tracked.phase, Phase::Conflict(_)))
    }

    fn untrack(&mut self, key: &str) {
        self.errors.remove(key);
        if let Some(tracked) = self.keys.remove(key) {
            self.set_dirty_bytes(self.dirty_bytes - tracked.size);
            self.pending.remove(&(tracked.since, key.to_owned()));
        }
    }

    fn status(&self) -> ShardStatus {
        let mut status = ShardStatus {
            flushing: self.flushing,
            dirty_bytes: self.dirty_bytes,
            oldest_pending: self.pending.first().map(|(since, _)| *since),
            last_error: self
                .errors
                .iter()
                .max_by_key(|(_, (number, _))| *number)
                .map(|(key, (_, error))| format!("{key}: {error}")),
            stopped: self.stopped,
            ..ShardStatus::default()
        };
        status.oldest_dirty = status.oldest_pending;
        for (key, tracked) in &self.keys {
            match &tracked.phase {
                Phase::Conflict(conflict) => status.conflicts.push(ConflictStatus {
                    key: key.clone(),
                    seq: conflict.seq,
                    remote_etag: conflict.remote_etag.clone(),
                    remote_identity: conflict.remote_identity.clone(),
                }),
                _ => {
                    status.dirty += 1;
                    continue;
                }
            }
            status.oldest_dirty = Some(
                status
                    .oldest_dirty
                    .map_or(tracked.since, |oldest| oldest.min(tracked.since)),
            );
        }
        status.conflicts.sort_by(|a, b| a.key.cmp(&b.key));
        status
    }
}

/// The flusher of one shard, running on the shard's primary (§7.1).
///
/// It follows the shard's applied client writes ([`Shard::subscribe`]),
/// after reading the dirty entries the index already holds, and flushes
/// each dirty key's latest committed version to the target:
///
/// - **Order and coalescing.** Keys are flushed oldest first, at most
///   [`FlushSettings::max_concurrency`](crate::FlushSettings::max_concurrency)
///   at once, and each key has at most one flush in flight. A flush sends
///   the key's latest version; versions committed during a flush are
///   flushed next, conditioned on what the flush put at the remote. Their
///   requests share the target's adaptive window with the other shards
///   flushing to it (§7.7).
/// - **States** (§4.2). A dirty key is taken for a flush (Dirty →
///   Flushing). A retryable failure puts it back after a backoff (Flushing
///   → Dirty), an out-of-band remote write puts it in conflict (Flushing
///   → Conflict), and success makes it clean once its `FLUSHED` record is
///   applied. Flushing and conflict are the primary's view, kept in memory:
///   the index shows such entries as dirty, and a restarted flusher finds
///   a conflict again by flushing, which is conditional and so safe.
/// - **Conflicts** (§7.2). The target's
///   [`conflict_policy`](Target::conflict_policy) decides: `hold` keeps
///   the key in conflict until [`ShardFlusher::resolve`], and `overwrite`
///   and `discard_local` resolve it at once (Conflict → Dirty). The next
///   flush of a resolved key sends it unconditionally under `overwrite`,
///   or adopts the remote's write under `discard_local`, with an `ADOPT`
///   that drops the local version (see the `conflict` module).
/// - **Recording.** The remote's answer is recorded with a `FLUSHED`
///   record committed lazily, on the next group commit
///   ([`Shard::commit_lazy`]). The flush slot is free before it is.
///
/// The flusher stops when its shard stops; dropping it stops it too.
#[derive(Debug)]
pub struct ShardFlusher {
    shard: ShardRef,
    state: Arc<Mutex<State>>,
    streams: Arc<Streams>,
    bodies: Arc<Bodies>,
    resolver: Resolver,
    task: JoinHandle<()>,
}

/// Sends operators' resolutions of held conflicts to a flusher's task.
#[derive(Debug, Clone)]
pub(crate) struct Resolver(mpsc::UnboundedSender<Resolve>);

impl Resolver {
    /// See [`ShardFlusher::resolve`].
    pub(crate) async fn resolve(
        &self,
        key: &str,
        policy: ConflictPolicy,
    ) -> Result<(), Unresolved> {
        let (reply, answer) = oneshot::channel();
        let resolve = Resolve {
            key: key.to_owned(),
            policy,
            reply,
        };
        self.0.send(resolve).map_err(|_| Unresolved::Stopped)?;
        answer.await.unwrap_or(Err(Unresolved::Stopped))
    }
}

/// An operator's resolution of a held conflict, and where its answer goes.
#[derive(Debug)]
struct Resolve {
    key: String,
    policy: ConflictPolicy,
    reply: oneshot::Sender<Result<(), Unresolved>>,
}

impl ShardFlusher {
    /// Starts flushing `shard` to `target` on the current Tokio runtime.
    pub fn spawn<S: ObjectStore, D: Disk>(shard: Shard<D>, target: Arc<Target<S>>) -> Self {
        let wall = Arc::clone(&target.wall);
        Self::start(shard, Ready::new(Some(target)), wall, None)
    }

    /// Starts tracking `shard`'s dirty keys at once, with their bytes
    /// counted against `charge`, and flushing them once `ready` holds the
    /// target. Ages are measured on `wall`.
    pub(crate) fn start<S: ObjectStore, D: Disk>(
        shard: Shard<D>,
        ready: Arc<Ready<S>>,
        wall: Arc<dyn WallClock>,
        charge: Option<Charge>,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            charge,
            ..State::default()
        }));
        let shard_ref = shard.shard().clone();
        let streams = Arc::new(Streams::default());
        let bodies = Arc::new(Bodies::default());
        let (commands, resolutions) = mpsc::unbounded_channel();
        let task = tokio::spawn(run(
            shard,
            ready,
            wall,
            Arc::clone(&state),
            (Arc::clone(&streams), Arc::clone(&bodies)),
            resolutions,
        ));
        Self {
            shard: shard_ref,
            state,
            streams,
            bodies,
            resolver: Resolver(commands),
            task,
        }
    }

    /// The shard flushed.
    #[must_use]
    pub fn shard(&self) -> &ShardRef {
        &self.shard
    }

    /// The flusher's state now.
    #[must_use]
    pub fn status(&self) -> ShardStatus {
        let mut status = lock(&self.state).status();
        status.streams = (self.streams.len() + self.bodies.len()) as u64;
        status.stopped |= self.task.is_finished();
        status
    }

    /// Where `key` is in its flush, or `None` if it is clean.
    #[must_use]
    pub fn phase(&self, key: &str) -> Option<Phase> {
        lock(&self.state)
            .keys
            .get(key)
            .map(|tracked| tracked.phase.clone())
    }

    /// Resolves the conflict `key` is held in under `policy`, as an
    /// operator asks (§7.2): the key returns to dirty, and its next flush
    /// retries the conditional request (`hold`), sends the local version
    /// unconditionally (`overwrite`), or adopts the remote's write and
    /// drops the local version (`discard_local`). Whether the bucket may
    /// discard is the caller's to check.
    ///
    /// # Errors
    ///
    /// [`Unresolved::NotHeld`] if the key is not held in conflict, and
    /// [`Unresolved::Stopped`] if the flusher stopped.
    pub async fn resolve(&self, key: &str, policy: ConflictPolicy) -> Result<(), Unresolved> {
        self.resolver.resolve(key, policy).await
    }

    /// The handle [`ShardFlusher::resolve`] sends through, if the flusher
    /// holds `key` in conflict.
    pub(crate) fn resolver_of(&self, key: &str) -> Option<Resolver> {
        matches!(self.phase(key), Some(Phase::Conflict(_))).then(|| self.resolver.clone())
    }

    /// Whether the flusher stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.task.is_finished()
    }

    /// Stops the flusher and waits for its task to end. Flushes in flight
    /// are abandoned; their keys stay dirty and are flushed again later.
    /// The `PART_FLUSHED` records of its streams that it started are
    /// committed before this returns, so a flusher started next resumes
    /// every remote upload this one knew of (§7.3).
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
        self.streams.recorded().await;
    }
}

impl Drop for ShardFlusher {
    fn drop(&mut self) {
        self.task.abort();
        lock(&self.state).stop();
    }
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    // Every update leaves the state consistent.
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a finished flush task returns: the key, the newest version known
/// when it started, how its conflict was resolved, and how it ended.
type Done = (String, EpochSeq, Option<ConflictPolicy>, Outcome);

/// What a finished `FLUSHED` commit returns: the key, the version
/// recorded, and whether the shard committed it.
type Recorded = (String, Seq, Result<(), ShardError>);

/// The flusher's task. It tracks the shard's dirty keys from the start,
/// and flushes them once the target is ready.
async fn run<S: ObjectStore, D: Disk>(
    shard: Shard<D>,
    ready: Arc<Ready<S>>,
    wall: Arc<dyn WallClock>,
    state: Arc<Mutex<State>>,
    (streams, bodies): (Arc<Streams>, Arc<Bodies>),
    mut resolutions: mpsc::UnboundedReceiver<Resolve>,
) {
    let mut changes = shard.subscribe();
    // A native target is ready from the start: its bodies are staged at the
    // peer, and no remote multipart upload is ever opened (§7.8).
    let native = ready.get().is_some_and(|target| target.native.is_some());
    // The streams first: an upload that completes in between is then a
    // dirty key the scan finds, not a closed stream of a clean key.
    let loaded = native || streams.load(&shard).await.is_ok();
    if loaded && scan(&shard, &*wall, &state).await.is_ok() {
        streams.reap_untracked(|key| lock(&state).keys.contains_key(key));
        let mut flushes: JoinSet<Done> = JoinSet::new();
        let mut records: JoinSet<Recorded> = JoinSet::new();
        let mut streaming: JoinSet<Step> = JoinSet::new();
        let mut staging: JoinSet<BodyStep> = JoinSet::new();
        // The flusher's place among those sending to the target, which
        // sizes the target's window (§7.7).
        let mut membership = None;
        loop {
            // Backoffs that ended are released on every turn, so that no
            // run of other events holds them back.
            lock(&state).release_due(Instant::now());
            // Waits for the target from before it is read, so that a target
            // set in between still wakes the loop.
            let set = ready.set.notified();
            tokio::pin!(set);
            set.as_mut().enable();
            let target = ready.get();
            if let Some(target) = &target {
                membership.get_or_insert_with(|| target.join());
                dispatch(&shard, target, &state, (&streams, &bodies), &mut flushes);
                if native {
                    bodies.pump(&shard, target, &mut staging);
                } else {
                    streams.pump(&shard, target, &mut streaming);
                }
            }
            let release = lock(&state).next_release();
            let timeout = target.as_ref().and_then(|target| {
                if native {
                    bodies.next_wake(&target.settings)
                } else {
                    streams.next_timeout(&target.settings)
                }
            });
            let staged = native.then_some(&*bodies);
            // Branches ready at once are taken in the order written, not at
            // random, so a simulation seed replays exactly. The flusher's
            // own tasks, bodies staged at a peer included, come first:
            // there are only as many as it started,
            // so the shard's changes, which a busy shard sends without end,
            // cannot hold them back. The timers only wake the loop, which
            // does their work on every turn.
            tokio::select! {
                biased;
                Some(done) = flushes.join_next(), if !flushes.is_empty() => {
                    // A flush runs only once there is a target.
                    if let (Ok(done), Some(target)) = (done, &target) {
                        let streamed = (&*streams, &*bodies);
                        finish(&shard, target, &state, streamed, &mut records, done);
                    }
                }
                Some(step) = streaming.join_next(), if !streaming.is_empty() => {
                    if let Ok(step) = step {
                        streams.finish(step);
                    }
                }
                Some(step) = staging.join_next(), if !staging.is_empty() => {
                    // A body is staged only once there is a target.
                    if let (Ok(step), Some(target)) = (step, &target) {
                        bodies.finish(step, &target.settings);
                    }
                }
                Some(recorded) = records.join_next(), if !records.is_empty() => {
                    if let Ok(recorded) = recorded
                        && !recorded_flush(&state, recorded)
                    {
                        break;
                    }
                }
                Some(resolve) = resolutions.recv() => {
                    resolved(&shard, &state, &mut records, resolve);
                }
                change = changes.recv() => match change {
                    Some(change) => follow(&state, &streams, staged, &*wall, change),
                    // The shard stopped, or another flusher took over.
                    None => break,
                },
                () = sleep_until(release), if release.is_some() => {}
                // The next pump gives up on the body whose `PUT` timed out.
                () = sleep_until(timeout), if timeout.is_some() => {}
                () = &mut set, if target.is_none() => {}
            }
        }
    }
    lock(&state).stop();
}

/// Acts on a change the shard applied. With a native target's `bodies`,
/// streamed bodies are staged at the peer, and multipart uploads are sent
/// once they complete (§7.8).
fn follow(
    state: &Mutex<State>,
    streams: &Streams,
    bodies: Option<&Bodies>,
    wall: &dyn WallClock,
    change: Change,
) {
    match (change, bodies) {
        (
            Change::Stored {
                key,
                position,
                size,
                upload,
            },
            bodies,
        ) => {
            if let Some(upload) = upload {
                match bodies {
                    Some(bodies) => bodies.completed(upload),
                    None => streams.completed(upload),
                }
            }
            let now = wall.now();
            let mut state = lock(state);
            state.changed(&key, position, size, now);
            if !state.awaits_flush(&key) {
                streams.reap(&key);
            }
        }
        (Change::Awaited(waiter), _) => lock(state).awaited(waiter),
        (Change::Streamed(body), Some(bodies)) => bodies.announced(body),
        (Change::Opened { .. } | Change::Part { .. } | Change::Aborted { .. }, Some(_)) => {}
        (Change::Opened { key, upload }, None) => streams.opened(&key, upload),
        (
            Change::Part {
                upload,
                number,
                position,
                ..
            },
            None,
        ) => streams.part(upload, number, position),
        (Change::Aborted { upload, .. }, None) => streams.aborted(upload),
        (Change::Streamed(body), None) => streams.announced(body),
    }
}

async fn sleep_until(at: Option<Instant>) {
    if let Some(at) = at {
        tokio::time::sleep_until(at).await;
    }
}

/// Tracks every entry the index holds that is not clean, as the flusher
/// starts. Changes applied meanwhile wait in the subscription.
async fn scan<D: Disk>(
    shard: &Shard<D>,
    wall: &dyn WallClock,
    state: &Mutex<State>,
) -> Result<(), ShardError> {
    let mut after = None;
    loop {
        let page = shard.entries(after.take(), SCAN_PAGE).await?;
        let now = wall.now();
        let mut state = lock(state);
        for (key, entry) in &page {
            if !matches!(entry.state, EntryState::Clean | EntryState::Evicted) {
                let (size, since) = dirty_since(entry, now);
                state.changed(key, entry.version, Some(size), since);
            }
        }
        match page.into_iter().last() {
            Some((key, _)) => after = Some(key),
            None => return Ok(()),
        }
    }
}

/// The size of an entry found dirty at startup, and when it became dirty:
/// its `Last-Modified`, or `now` for a tombstone, which has none.
fn dirty_since(entry: &Entry, now: Duration) -> (u64, Duration) {
    match &entry.object {
        Some(object) => (
            object.size,
            Duration::from_millis(object.last_modified_ms).min(now),
        ),
        None => (0, now),
    }
}

/// Starts flushes of dirty keys while slots are free.
fn dispatch<S: ObjectStore, D: Disk>(
    shard: &Shard<D>,
    target: &Arc<Target<S>>,
    state: &Arc<Mutex<State>>,
    (streams, bodies): (&Arc<Streams>, &Arc<Bodies>),
    flushes: &mut JoinSet<Done>,
) {
    while flushes.len() < target.settings.max_concurrency as usize {
        let (key, dispatched, resolution, pending) = {
            let mut state = lock(state);
            let Some((key, dispatched, resolution)) = state.take_ready() else {
                return;
            };
            let pending = state.recording.get(&key).cloned();
            (key, dispatched, resolution, pending)
        };
        let (shard, target, streams) = (shard.clone(), Arc::clone(target), Arc::clone(streams));
        let bodies = Arc::clone(bodies);
        let early = early_answer(state, &key, dispatched);
        flushes.spawn(async move {
            let attempt = Attempt {
                shard: &shard,
                target: &target,
                key: &key,
                streams: &streams,
                resolution,
                bodies: &bodies,
                early: early.as_deref(),
            };
            let outcome = attempt.run(pending.as_ref()).await;
            (key, dispatched, resolution, outcome)
        });
    }
}

/// The seeded bug [`PeerBug::AnsweredOnCommit`]: what answers the
/// write-through writes waiting for `key`, up to `version`, once its
/// `COMMIT` is sent.
fn early_answer(
    state: &Arc<Mutex<State>>,
    key: &str,
    version: EpochSeq,
) -> Option<Box<dyn Fn() + Send + Sync>> {
    if peer_hooks::peer_bug() != PeerBug::AnsweredOnCommit {
        return None;
    }
    let (state, key) = (Arc::clone(state), key.to_owned());
    Some(Box::new(move || lock(&state).answer_waits(&key, version)))
}

/// Acts on a finished flush.
fn finish<S, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    state: &Mutex<State>,
    (streams, bodies): (&Streams, &Bodies),
    records: &mut JoinSet<Recorded>,
    (key, dispatched, resolution, outcome): Done,
) {
    let mut state = lock(state);
    match outcome {
        Outcome::Flushed {
            version,
            remote,
            record,
        } => {
            target.counters.flushes.inc();
            if resolution == Some(ConflictPolicy::Overwrite) {
                target.counters.overwritten.inc();
                tracing::warn!(shard = %shard.shard(), key, seq = %version.seq,
                    "the local version overwrote an out-of-band write");
            }
            state.reached(&key, version);
            if record {
                let flushed = Flushed {
                    key: key.clone(),
                    seq: version.seq,
                    remote_etag: remote.etag.clone(),
                    remote_version_id: remote.version_id.clone(),
                };
                if target.native.is_some() {
                    // A delete's `COMMIT` carries the identity of its
                    // tombstone; a version's, the one its record names.
                    let identity = remote
                        .version_id
                        .clone()
                        .unwrap_or_else(|| target.identity(shard.shard(), version).to_string());
                    peer_hooks::flushed(&FlushedRecord {
                        shard: shard.shard().clone(),
                        key: key.clone(),
                        version,
                        identity,
                        delete: remote.version_id.is_none(),
                    });
                }
                state.recording.insert(key.clone(), remote);
                let (shard, recorded) = (shard.clone(), key.clone());
                records.spawn(async move {
                    let committed = shard.commit_lazy(RecordBody::Flushed(flushed)).await;
                    (recorded, version.seq, committed.map(drop))
                });
                state.finished(&key, dispatched);
            } else {
                // A tombstone stays until the import passes its key
                // (§4.2): look again later, less often the longer it waits.
                let at = Instant::now() + target.settings.backoff(state.wait(&key));
                state.failed(&key, "waiting for the import".to_owned(), at);
            }
        }
        Outcome::Settled => {
            // The remote holds the key's latest version, read after the
            // flush was dispatched.
            state.reached(&key, dispatched);
            state.finished(&key, dispatched);
        }
        Outcome::Conflict(conflict) => {
            target.counters.conflicts.inc();
            let policy = target.conflict_policy;
            tracing::warn!(shard = %shard.shard(), key, seq = %conflict.seq, ?policy,
                "the remote changed out of band; the key is in conflict");
            state.conflicted(&key, conflict, policy);
        }
        Outcome::Discarded { version } => {
            target.counters.discarded.inc();
            tracing::warn!(shard = %shard.shard(), key, seq = %version.seq,
                "an acknowledged write was dropped for an out-of-band write");
            state.discarded(&key, version);
            state.finished(&key, dispatched);
        }
        Outcome::Retry(error) => {
            target.counters.retries.inc();
            tracing::debug!(shard = %shard.shard(), key, %error, "a flush will be retried");
            let at = Instant::now() + target.settings.backoff(state.wait(&key));
            state.failed(&key, error, at);
        }
    }
    if !state.awaits_flush(&key) {
        streams.reap(&key);
        bodies.reap(&key);
    }
}

/// Carries out an operator's resolution of a held conflict (§7.2), and
/// answers it.
fn resolved<D: Disk>(
    shard: &Shard<D>,
    state: &Mutex<State>,
    records: &mut JoinSet<Recorded>,
    Resolve { key, policy, reply }: Resolve,
) {
    let mut state = lock(state);
    let result = if conflict_bug() == ConflictBug::ResolvesClean {
        state.resolve_clean(&key).map(|(seq, conflict)| {
            // The seeded bug: records the local version as flushed, over
            // the remote's write.
            let shard = shard.clone();
            let recorded = key.clone();
            records.spawn(async move {
                let entry = shard.entry(&recorded).await;
                let tombstone = matches!(&entry, Ok(Some(entry)) if entry.object.is_none());
                let flushed = Flushed {
                    key: recorded.clone(),
                    seq,
                    remote_etag: conflict.remote_etag.filter(|_| !tombstone),
                    remote_version_id: None,
                };
                let committed = shard.commit_lazy(RecordBody::Flushed(flushed)).await;
                (recorded, seq, committed.map(drop))
            });
        })
    } else {
        state.resolve(&key, policy)
    };
    if result.is_ok() {
        tracing::info!(shard = %shard.shard(), key, ?policy, "a held conflict was resolved");
    }
    // The operator may have stopped waiting.
    let _ = reply.send(result);
}

/// Acts on a finished `FLUSHED` commit, and returns whether the flusher
/// should go on. A commit fails only if the shard stopped or broke; the
/// flusher then stops too, and a new one, started once the shard is open
/// again, finds the remote already holds the version (§7.1).
fn recorded_flush(state: &Mutex<State>, (key, seq, result): Recorded) -> bool {
    let mut state = lock(state);
    if state.recording.get(&key).is_some_and(|r| r.seq == seq) {
        state.recording.remove(&key);
    }
    if let Err(error) = &result {
        tracing::warn!(key, %error, "a FLUSHED was not committed; the flusher stops");
        state.record_error(&key, error.to_string());
    }
    result.is_ok()
}

#[cfg(test)]
mod tests {
    use skys3_shard::FlushWait;
    use skys3_types::Epoch;

    use super::*;

    fn position(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    fn conflict() -> Conflict {
        Conflict {
            seq: Seq::new(1),
            remote_etag: Some(ETag::new("theirs").unwrap()),
            remote_identity: None,
        }
    }

    /// A state tracking `k`, dirty at seq 1, whose flush was just taken.
    fn flushing() -> State {
        let mut state = State::default();
        state.changed("k", position(1), Some(10), Duration::from_secs(1));
        assert_eq!(
            state.take_ready(),
            Some(("k".to_owned(), position(1), None))
        );
        state
    }

    fn phase(state: &State) -> Phase {
        state.keys["k"].phase.clone()
    }

    /// The answer `wait` has now, if any.
    fn answer_now(wait: &mut FlushWait) -> Option<FlushState> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(1), wait.answered())
                .await
                .ok()
                .flatten()
        })
    }

    #[test]
    fn hold_keeps_the_key_until_it_is_resolved_back_to_dirty() {
        let mut state = flushing();
        state.conflicted("k", conflict(), ConflictPolicy::Hold);
        assert_eq!(phase(&state), Phase::Conflict(conflict()));
        assert!(state.ready.is_empty() && state.pending.is_empty());
        assert_eq!(state.status().conflicts.len(), 1);
        assert_eq!(
            state.resolve("other", ConflictPolicy::Overwrite),
            Err(Unresolved::NotHeld)
        );

        // Conflict → Dirty, in line again, with the resolution its next
        // flush carries out.
        state.resolve("k", ConflictPolicy::Overwrite).unwrap();
        assert_eq!(phase(&state), Phase::Dirty);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(
            state.take_ready(),
            Some(("k".to_owned(), position(1), Some(ConflictPolicy::Overwrite)))
        );
        // A failed attempt keeps the resolution; a flush ends it.
        let now = Instant::now();
        state.failed("k", "the remote failed".to_owned(), now);
        state.release_due(now);
        let (_, _, resolution) = state.take_ready().unwrap();
        assert_eq!(resolution, Some(ConflictPolicy::Overwrite));
        state.changed("k", position(2), Some(5), Duration::from_secs(2));
        state.finished("k", position(1));
        assert_eq!(
            state.take_ready(),
            Some(("k".to_owned(), position(2), None))
        );
        assert_eq!(
            state.resolve("k", ConflictPolicy::Hold),
            Err(Unresolved::NotHeld),
            "a key being flushed is not held"
        );
    }

    #[test]
    fn other_policies_resolve_at_once() {
        for policy in [ConflictPolicy::Overwrite, ConflictPolicy::DiscardLocal] {
            let mut state = flushing();
            let (waiter, mut wait) = FlushWaiter::new("k".to_owned(), position(1));
            state.awaited(waiter);
            state.conflicted("k", conflict(), policy);
            assert_eq!(phase(&state), Phase::Dirty);
            assert!(state.status().conflicts.is_empty());
            assert_eq!(
                state.take_ready(),
                Some(("k".to_owned(), position(1), Some(policy)))
            );
            // A write waiting for the version learns of the conflict only
            // if the version will be dropped.
            let expected = (policy == ConflictPolicy::DiscardLocal).then_some(FlushState::Conflict);
            assert_eq!(answer_now(&mut wait), expected, "{policy:?}");
        }
    }

    #[test]
    fn a_discard_answers_the_writes_waiting_for_what_it_dropped() {
        let mut state = flushing();
        state.changed("k", position(3), Some(5), Duration::from_secs(2));
        let (dropped, mut dropped_wait) = FlushWaiter::new("k".to_owned(), position(1));
        let (later, mut later_wait) = FlushWaiter::new("k".to_owned(), position(3));
        state.awaited(dropped);
        state.awaited(later);
        state.discarded("k", position(1));
        assert_eq!(answer_now(&mut dropped_wait), Some(FlushState::Conflict));
        assert_eq!(answer_now(&mut later_wait), None);
        assert_eq!(state.keys["k"].waiters.len(), 1);
    }

    #[test]
    fn the_seeded_bug_untracks_a_held_key() {
        let mut state = flushing();
        assert_eq!(state.resolve_clean("k"), Err(Unresolved::NotHeld));
        state.conflicted("k", conflict(), ConflictPolicy::Hold);
        assert_eq!(state.resolve_clean("k"), Ok((Seq::new(1), conflict())));
        assert!(state.keys.is_empty() && state.dirty_bytes == 0);
    }
}
