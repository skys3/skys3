//! The flusher of one shard (§7.1): which keys are dirty, which are being
//! flushed, which are held in conflict, and the task that drives them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_index::{Entry, EntryState};
use skys3_io::{Disk, WallClock};
use skys3_log::record::Flushed;
use skys3_log::{RecordBody, ShardRef};
use skys3_remote::ObjectStore;
use skys3_shard::{Change, Shard, ShardError};
use skys3_types::{ETag, EpochSeq, Seq};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::attempt::{Attempt, Conflict, Outcome, Remote};
use crate::budget::Charge;
use crate::stream::{Step, Streams};
use crate::target::Target;

/// The target a flusher sends to, once its capability probe is done.
pub(crate) type Ready<S> = watch::Receiver<Option<Arc<Target<S>>>>;

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
    /// that are open: not completed or aborted yet.
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
        if let Some(charge) = self.charge.take() {
            charge.adjust(self.dirty_bytes, 0);
        }
    }

    /// Takes the next dirty key for a flush, if any.
    fn take_ready(&mut self) -> Option<(String, EpochSeq)> {
        let (_, key) = self.ready.pop_first()?;
        let tracked = self.keys.get_mut(&key)?;
        tracked.phase = Phase::Flushing;
        tracked.newer_since = None;
        self.flushing += 1;
        Some((key, tracked.latest))
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

    /// Ends the flush of `key` in conflict: it is held, and no longer in
    /// line.
    fn conflicted(&mut self, key: &str, conflict: Conflict) {
        self.flushing -= 1;
        self.errors.remove(key);
        if let Some(tracked) = self.keys.get_mut(key) {
            self.pending.remove(&(tracked.since, key.to_owned()));
            tracked.phase = Phase::Conflict(conflict);
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
///   [`FlushSettings::concurrency`](crate::FlushSettings::concurrency) at
///   once, and each key has at most one flush in flight. A flush sends the
///   key's latest version; versions committed during a flush are flushed
///   next, conditioned on what the flush put at the remote.
/// - **States** (§4.2). A dirty key is taken for a flush (Dirty →
///   Flushing). A retryable failure puts it back after a backoff (Flushing
///   → Dirty), an out-of-band remote write holds it in conflict (Flushing
///   → Conflict), and success makes it clean once its `FLUSHED` record is
///   applied. Flushing and conflict are the primary's view, kept in memory:
///   the index shows such entries as dirty, and a restarted flusher finds
///   a conflict again by flushing, which is conditional and so safe.
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
    task: JoinHandle<()>,
}

impl ShardFlusher {
    /// Starts flushing `shard` to `target` on the current Tokio runtime.
    pub fn spawn<S: ObjectStore, D: Disk>(shard: Shard<D>, target: Arc<Target<S>>) -> Self {
        let wall = Arc::clone(&target.wall);
        // The sender may go: the target is ready for good.
        let (_, ready) = watch::channel(Some(target));
        Self::start(shard, ready, wall, None)
    }

    /// Starts tracking `shard`'s dirty keys at once, with their bytes
    /// counted against `charge`, and flushing them once `ready` holds the
    /// target. Ages are measured on `wall`.
    pub(crate) fn start<S: ObjectStore, D: Disk>(
        shard: Shard<D>,
        ready: Ready<S>,
        wall: Arc<dyn WallClock>,
        charge: Option<Charge>,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            charge,
            ..State::default()
        }));
        let shard_ref = shard.shard().clone();
        let streams = Arc::new(Streams::default());
        let task = tokio::spawn(run(
            shard,
            ready,
            wall,
            Arc::clone(&state),
            Arc::clone(&streams),
        ));
        Self {
            shard: shard_ref,
            state,
            streams,
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
        status.streams = self.streams.len() as u64;
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
/// when it started, and how it ended.
type Done = (String, EpochSeq, Outcome);

/// What a finished `FLUSHED` commit returns: the key, the version
/// recorded, and whether the shard committed it.
type Recorded = (String, Seq, Result<(), ShardError>);

/// The flusher's task. It tracks the shard's dirty keys from the start,
/// and flushes them once the target is ready.
async fn run<S: ObjectStore, D: Disk>(
    shard: Shard<D>,
    mut ready: Ready<S>,
    wall: Arc<dyn WallClock>,
    state: Arc<Mutex<State>>,
    streams: Arc<Streams>,
) {
    let mut changes = shard.subscribe();
    // The streams first: an upload that completes in between is then a
    // dirty key the scan finds, not a closed stream of a clean key.
    if streams.load(&shard).await.is_ok() && scan(&shard, &*wall, &state).await.is_ok() {
        streams.reap_untracked(|key| lock(&state).keys.contains_key(key));
        let mut flushes: JoinSet<Done> = JoinSet::new();
        let mut records: JoinSet<Recorded> = JoinSet::new();
        let mut streaming: JoinSet<Step> = JoinSet::new();
        // Whether a target may still arrive.
        let mut awaiting = true;
        loop {
            let target = ready.borrow_and_update().clone();
            if let Some(target) = &target {
                dispatch(&shard, target, &state, &streams, &mut flushes);
                streams.pump(&shard, target, &mut streaming);
            }
            let release = lock(&state).next_release();
            let timeout = target
                .as_ref()
                .and_then(|target| streams.next_timeout(&target.settings));
            tokio::select! {
                change = changes.recv() => match change {
                    Some(change) => follow(&state, &streams, &*wall, change),
                    // The shard stopped, or another flusher took over.
                    None => break,
                },
                Some(done) = flushes.join_next(), if !flushes.is_empty() => {
                    // A flush runs only once there is a target.
                    if let (Ok(done), Some(target)) = (done, &target) {
                        finish(&shard, target, &state, &streams, &mut records, done);
                    }
                }
                Some(step) = streaming.join_next(), if !streaming.is_empty() => {
                    if let Ok(step) = step {
                        streams.finish(step);
                    }
                }
                Some(recorded) = records.join_next(), if !records.is_empty() => {
                    if let Ok(recorded) = recorded
                        && !recorded_flush(&state, recorded)
                    {
                        break;
                    }
                }
                () = sleep_until(release), if release.is_some() => {
                    lock(&state).release_due(Instant::now());
                }
                // The next pump gives up on the body whose `PUT` timed out.
                () = sleep_until(timeout), if timeout.is_some() => {}
                changed = ready.changed(), if target.is_none() && awaiting => {
                    awaiting = changed.is_ok();
                }
            }
        }
    }
    lock(&state).stop();
}

/// Acts on a change the shard applied.
fn follow(state: &Mutex<State>, streams: &Streams, wall: &dyn WallClock, change: Change) {
    match change {
        Change::Stored {
            key,
            position,
            size,
            upload,
        } => {
            if let Some(upload) = upload {
                streams.completed(upload);
            }
            let now = wall.now();
            let mut state = lock(state);
            state.changed(&key, position, size, now);
            if !state.awaits_flush(&key) {
                streams.reap(&key);
            }
        }
        Change::Opened { key, upload } => streams.opened(&key, upload),
        Change::Part {
            upload,
            number,
            position,
            ..
        } => streams.part(upload, number, position),
        Change::Aborted { upload, .. } => streams.aborted(upload),
        Change::Streamed(body) => streams.announced(body),
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
    state: &Mutex<State>,
    streams: &Arc<Streams>,
    flushes: &mut JoinSet<Done>,
) {
    while flushes.len() < target.settings.concurrency {
        let (key, dispatched, pending) = {
            let mut state = lock(state);
            let Some((key, dispatched)) = state.take_ready() else {
                return;
            };
            let pending = state.recording.get(&key).cloned();
            (key, dispatched, pending)
        };
        let (shard, target, streams) = (shard.clone(), Arc::clone(target), Arc::clone(streams));
        flushes.spawn(async move {
            let attempt = Attempt {
                shard: &shard,
                target: &target,
                key: &key,
                streams: &streams,
            };
            let outcome = attempt.run(pending.as_ref()).await;
            (key, dispatched, outcome)
        });
    }
}

/// Acts on a finished flush.
fn finish<S, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    state: &Mutex<State>,
    streams: &Streams,
    records: &mut JoinSet<Recorded>,
    (key, dispatched, outcome): Done,
) {
    let mut state = lock(state);
    match outcome {
        Outcome::Flushed {
            version,
            remote,
            record,
        } => {
            target.counters.flushes.inc();
            if record {
                let flushed = Flushed {
                    key: key.clone(),
                    seq: version.seq,
                    remote_etag: remote.etag.clone(),
                    remote_version_id: remote.version_id.clone(),
                };
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
        Outcome::Settled => state.finished(&key, dispatched),
        Outcome::Conflict(conflict) => {
            target.counters.conflicts.inc();
            tracing::warn!(shard = %shard.shard(), key, seq = %conflict.seq,
                "the remote changed out of band; the key is held in conflict");
            state.conflicted(&key, conflict);
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
    }
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
