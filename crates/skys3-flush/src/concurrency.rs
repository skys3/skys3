//! Adaptive flush concurrency (§7.7): how many requests the flushers of one
//! target keep in flight at once.
//!
//! - **The window.** Every request the flushers of a target send takes a
//!   slot of the target's window before it is sent, and gives it back when
//!   its answer arrives ([`Paced`]). Requests that find the window full
//!   wait in order, whichever shard sent them, so the shards a node leads
//!   share the window first come, first served. A link has one
//!   bandwidth-delay product however many shards use it, so the window is
//!   one number per target, and the per-shard keys scale its bounds: it
//!   lies between `flush_min_concurrency_per_shard` and
//!   `flush_max_concurrency_per_shard` times the number of the target's
//!   shards whose flusher runs on the node ([`Window::join`]).
//! - **The control signal.** An answered request (a success, or a `4xx`
//!   that is not a throttle) is a latency sample, from the moment it is
//!   sent, holding its slot, until its answer arrives. Samples are grouped
//!   into rounds: a round ends when a request sent after it began is
//!   answered, about one round trip. The base round trip is the smallest
//!   round mean of the last [`BASE_ROUNDS`] rounds, a windowed minimum
//!   filter: the latency of a round in which nothing queued.
//! - **Additive increase.** A round whose mean stays within
//!   [`LATENCY_TOLERANCE`] of the base round trip, and at whose end
//!   requests wait for a slot, grows the window: it doubles in the slow
//!   start, until the first decrease, and grows by one request after, but
//!   never past the requests in flight and waiting. By Little's law
//!   throughput is the requests in flight divided by their latency, so
//!   while the window is full and latency stays at the base, throughput
//!   rises with every slot added; once it no longer does, latency rises
//!   instead. A round that ends with no request waiting, because there is
//!   no more to send or the in-flight byte bound holds requests back
//!   before they ask for a slot, does not grow the window.
//! - **Multiplicative decrease.** A round whose mean exceeds the
//!   tolerance shrinks the window to [`LATENCY_TARGET`] of the size at
//!   which its mean would have been the base round trip, at most halving
//!   it ([`LATENCY_MIN_FACTOR`]): just below the knee, so that the next
//!   rounds measure the base round trip again and the filter keeps it. A
//!   throttle ([`S3Error::is_throttle`]: `503 SlowDown`, any other `503`,
//!   `429`, or a throttling code) shrinks it by [`THROTTLE_DECREASE`] at
//!   once, at most once per window of requests: throttles of requests sent
//!   before the last decrease are not counted again. The round after a
//!   decrease only measures. Other failures say nothing about the window.
//!
//! The window never leaves its bounds, so with equal bounds it is fixed.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, ObjectStore, PutObject, S3Error, S3Result, UploadId, UploadPart,
    WriteOutput,
};
use skys3_types::ETag;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::peer::Stamp;

/// The rounds over which the base round trip is the smallest round mean.
/// After a decrease for rising latency, the window grows back past the
/// knee in about a third of the window's size in rounds, so the base is
/// measured again well within this many rounds for any window up to a few
/// thousand requests. A path whose round trip grows for good is seen as
/// congested until its old base ages out.
pub const BASE_ROUNDS: u64 = 1024;

/// How far above the base round trip a round's mean latency may rise
/// before the window shrinks: by a quarter.
pub const LATENCY_TOLERANCE: f64 = 0.25;

/// Where a decrease for rising latency aims: this fraction of the window
/// at which the round's mean would have been the base round trip.
pub const LATENCY_TARGET: f64 = 0.9;

/// The smallest factor a decrease for rising latency applies: it at most
/// halves the window, as after a slow start that overshot.
pub const LATENCY_MIN_FACTOR: f64 = 0.5;

/// The factor a throttle shrinks the window by.
pub const THROTTLE_DECREASE: f64 = 0.7;

/// What the answer to one request says about the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Signal {
    /// The remote answered after this long.
    Answered(Duration),
    /// The remote asked to send less.
    Throttled,
    /// No answer, or a server error: nothing to learn.
    Failed,
}

impl Signal {
    /// The signal of `result`, answered `latency` after it was sent.
    pub(crate) fn of<T>(result: &S3Result<T>, latency: Duration) -> Signal {
        match result {
            Ok(_) => Signal::Answered(latency),
            Err(error) => Self::of_error(error, latency),
        }
    }

    fn of_error(error: &S3Error, latency: Duration) -> Signal {
        if error.is_throttle() {
            Signal::Throttled
        } else if matches!(error.status(), Some(400..=499)) {
            // A refusal, such as a failed precondition, is a full answer.
            Signal::Answered(latency)
        } else {
            Signal::Failed
        }
    }
}

/// The samples of the current round.
#[derive(Debug, Default)]
struct Round {
    samples: u32,
    total: Duration,
}

/// The additive-increase, multiplicative-decrease controller of one
/// target's window: pure state, fed the requests sent and their answers.
#[derive(Debug)]
pub(crate) struct Controller {
    limit: u32,
    floor: u32,
    ceiling: u32,
    slow_start: bool,
    /// The number of the next request sent.
    next: u64,
    /// The first request of the current round: the round ends when a
    /// request numbered from it is answered.
    round_start: u64,
    /// Throttles of requests numbered below it were acted on already.
    recovery: u64,
    /// Whether the current round only measures, after a decrease.
    settling: bool,
    round: Round,
    /// Rounds ended so far.
    rounds: u64,
    /// The round means that may still become the minimum, with the round
    /// that measured them: increasing means, the minimum first.
    minima: VecDeque<(u64, Duration)>,
}

impl Controller {
    /// A controller between `floor` and `ceiling` requests, starting at
    /// `floor`, in slow start.
    pub(crate) fn new(floor: u32, ceiling: u32) -> Self {
        let floor = floor.max(1);
        Self {
            limit: floor,
            floor,
            ceiling: ceiling.max(floor),
            slow_start: true,
            next: 0,
            round_start: 0,
            recovery: 0,
            settling: false,
            round: Round::default(),
            rounds: 0,
            minima: VecDeque::new(),
        }
    }

    /// The requests the window allows in flight.
    pub(crate) fn limit(&self) -> u32 {
        self.limit
    }

    /// The bounds of the window.
    pub(crate) fn bounds(&self) -> (u32, u32) {
        (self.floor, self.ceiling)
    }

    /// The base round trip, once a round has ended.
    pub(crate) fn base(&self) -> Option<Duration> {
        self.minima.front().map(|&(_, mean)| mean)
    }

    /// Moves the bounds, keeping the window within them.
    pub(crate) fn set_bounds(&mut self, floor: u32, ceiling: u32) {
        self.floor = floor.max(1);
        self.ceiling = ceiling.max(self.floor);
        self.limit = self.limit.clamp(self.floor, self.ceiling);
    }

    /// Numbers a request being sent.
    pub(crate) fn sent(&mut self) -> u64 {
        let number = self.next;
        self.next += 1;
        number
    }

    /// Learns from the answer to request `number`, while `demand`
    /// requests, this one included, are in flight or waiting for a slot.
    pub(crate) fn answered(&mut self, number: u64, signal: Signal, demand: u32) {
        match signal {
            Signal::Failed => {}
            Signal::Throttled => {
                if number >= self.recovery && hooks::bug() != ConcurrencyBug::IgnoresThrottles {
                    self.decrease(THROTTLE_DECREASE);
                }
            }
            Signal::Answered(latency) => {
                self.round.samples += 1;
                self.round.total = self.round.total.saturating_add(latency);
                if number >= self.round_start {
                    self.end_round(demand);
                }
            }
        }
    }

    fn end_round(&mut self, demand: u32) {
        let round = std::mem::take(&mut self.round);
        let mean = round.total / round.samples.max(1);
        self.rounds += 1;
        while self.minima.back().is_some_and(|&(_, kept)| kept >= mean) {
            self.minima.pop_back();
        }
        self.minima.push_back((self.rounds, mean));
        while self
            .minima
            .front()
            .is_some_and(|&(at, _)| at + BASE_ROUNDS <= self.rounds)
        {
            self.minima.pop_front();
        }
        let base = self.base().unwrap_or(mean);
        if std::mem::take(&mut self.settling) {
            // The round began before the decrease took effect.
        } else if mean > base.mul_f64(1.0 + LATENCY_TOLERANCE) {
            let knee = base.as_secs_f64() / mean.as_secs_f64();
            self.decrease((LATENCY_TARGET * knee).max(LATENCY_MIN_FACTOR));
            return;
        } else if demand > self.limit {
            let grown = if self.slow_start {
                self.limit.saturating_mul(2)
            } else {
                self.limit.saturating_add(1)
            };
            self.limit = grown.min(demand).min(self.ceiling);
        }
        self.round_start = self.next;
    }

    /// Shrinks the window by `factor`, ends the slow start, and starts a
    /// round that only measures.
    fn decrease(&mut self, factor: f64) {
        // Truncating: a window of a few requests still shrinks.
        let shrunk = (f64::from(self.limit) * factor) as u32;
        self.limit = shrunk.max(self.floor);
        self.slow_start = false;
        self.recovery = self.next;
        self.round = Round::default();
        self.round_start = self.next;
        self.settling = true;
    }
}

/// One target's window as its flushers' requests see it.
pub(crate) struct Window {
    /// The free slots, less those owed after a decrease below the
    /// requests in flight.
    permits: Semaphore,
    state: Mutex<WindowState>,
    /// Counts throttles, in the target's metrics once it has them.
    throttles: Mutex<Counter>,
}

#[derive(Debug)]
struct WindowState {
    controller: Controller,
    /// `flush_min_concurrency_per_shard` and
    /// `flush_max_concurrency_per_shard`.
    per_shard: (u32, u32),
    /// The flushers sending to the target.
    shards: u32,
    in_flight: u32,
    /// Requests waiting for a slot.
    waiting: u32,
    /// Slots to keep, rather than free, as requests end: the window shrank
    /// below the requests in flight. Free slots, plus requests in flight,
    /// are always the limit plus the debt.
    debt: u32,
}

impl fmt::Debug for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Window")
            .field("state", &*self.lock())
            .finish_non_exhaustive()
    }
}

/// The window of a target at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyStatus {
    /// The requests the flushers may have in flight to the target now.
    pub limit: u32,
    /// The requests in flight.
    pub in_flight: u32,
    /// The least the window may be: `flush_min_concurrency_per_shard`
    /// times the flushers sending to the target.
    pub floor: u32,
    /// The most it may be: `flush_max_concurrency_per_shard` times the
    /// flushers sending to the target.
    pub ceiling: u32,
    /// The base round trip the window is sized against, once measured.
    pub base_round_trip: Option<Duration>,
    /// Bytes the flushers hold for requests to the target, bounded by
    /// `flush_max_inflight_bytes_per_target`.
    pub inflight_bytes: u64,
}

impl Window {
    /// A window for `per_shard` (the least and most requests per shard),
    /// sized for one shard until flushers join it.
    pub(crate) fn new(per_shard: (u32, u32)) -> Self {
        let (least, most) = (per_shard.0.max(1), per_shard.1.max(per_shard.0).max(1));
        let controller = Controller::new(least, most);
        Self {
            permits: Semaphore::new(controller.limit() as usize),
            state: Mutex::new(WindowState {
                controller,
                per_shard: (least, most),
                shards: 0,
                in_flight: 0,
                waiting: 0,
                debt: 0,
            }),
            throttles: Mutex::new(Counter::default()),
        }
    }

    /// Counts throttles in `counter`.
    pub(crate) fn count_throttles(&self, counter: Counter) {
        *self
            .throttles
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = counter;
    }

    /// Adds a flusher to the shards sending to the target, widening the
    /// bounds, until the returned membership is dropped.
    pub(crate) fn join(self: &Arc<Self>) -> Membership {
        self.update(|state| state.shards += 1);
        Membership(Arc::clone(self))
    }

    /// The window now; `inflight_bytes` is the caller's to fill in.
    pub(crate) fn status(&self) -> ConcurrencyStatus {
        let state = self.lock();
        let (floor, ceiling) = state.controller.bounds();
        ConcurrencyStatus {
            limit: state.controller.limit(),
            in_flight: state.in_flight,
            floor,
            ceiling,
            base_round_trip: state.controller.base(),
            inflight_bytes: 0,
        }
    }

    /// Sends `request` once it has a slot, and learns from its answer.
    pub(crate) async fn send<T>(&self, request: impl Future<Output = S3Result<T>>) -> S3Result<T> {
        let waiting = Waiting::new(self);
        // The semaphore is never closed.
        if let Ok(permit) = self.permits.acquire().await {
            permit.forget();
        }
        drop(waiting);
        let number = self.update(|state| {
            state.in_flight += 1;
            state.controller.sent()
        });
        let mut slot = Slot {
            window: self,
            number: Some(number),
        };
        let sent = Instant::now();
        let result = request.await;
        let signal = Signal::of(&result, sent.elapsed());
        if signal == Signal::Throttled {
            self.throttles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .inc();
        }
        slot.release(Some(signal));
        result
    }

    /// Changes the state with `change`, then frees or takes back the slots
    /// that the limit gained or lost.
    fn update<T>(&self, change: impl FnOnce(&mut WindowState) -> T) -> T {
        let mut state = self.lock();
        let before = state.controller.limit();
        let result = change(&mut state);
        let shards = state.shards.max(1);
        let (least, most) = state.per_shard;
        state
            .controller
            .set_bounds(least.saturating_mul(shards), most.saturating_mul(shards));
        let after = state.controller.limit();
        if after > before {
            let grown = after - before;
            let repaid = grown.min(state.debt);
            state.debt -= repaid;
            self.permits.add_permits((grown - repaid) as usize);
        } else if before > after {
            let shrunk = before - after;
            let taken = self.permits.forget_permits(shrunk as usize);
            state.debt += shrunk - u32::try_from(taken).unwrap_or(shrunk);
        }
        result
    }

    fn lock(&self) -> MutexGuard<'_, WindowState> {
        // Every update leaves the state consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A request waiting for a slot, counted until dropped.
struct Waiting<'w>(&'w Window);

impl<'w> Waiting<'w> {
    fn new(window: &'w Window) -> Self {
        window.lock().waiting += 1;
        Self(window)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.lock().waiting -= 1;
    }
}

/// A slot of the window, held by a request in flight. Dropping it, as when
/// the flush is abandoned, frees it without a sample.
struct Slot<'w> {
    window: &'w Window,
    /// The request's number, until the slot is freed.
    number: Option<u64>,
}

impl Slot<'_> {
    /// Frees the slot, once, learning from `signal`.
    fn release(&mut self, signal: Option<Signal>) {
        let Some(number) = self.number.take() else {
            return;
        };
        self.window.update(|state| {
            if let Some(signal) = signal {
                let demand = state.in_flight.saturating_add(state.waiting);
                state.controller.answered(number, signal, demand);
            }
            state.in_flight -= 1;
            if state.debt > 0 {
                state.debt -= 1;
            } else {
                self.window.permits.add_permits(1);
            }
        });
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.release(None);
    }
}

/// A flusher's place among the shards sending to a target; dropping it
/// leaves, narrowing the bounds.
#[derive(Debug)]
pub(crate) struct Membership(Arc<Window>);

impl Drop for Membership {
    fn drop(&mut self) {
        self.0
            .update(|state| state.shards = state.shards.saturating_sub(1));
    }
}

/// A target's store, as its flushers send to it: each request through the
/// target's [`Window`], and each write that publishes or deletes a version
/// stamped with its apply-by time as it is sent, for a target that may be
/// a SkyS3 peer (§7.8).
pub(crate) struct Paced<S> {
    inner: Stamping<S>,
    window: Arc<Window>,
}

impl<S> Paced<S> {
    pub(crate) fn new(inner: Arc<S>, window: Arc<Window>) -> Self {
        Self {
            inner: Stamping::new(inner, None),
            window,
        }
    }

    /// Stamps every write that publishes or deletes a version with
    /// `stamp`.
    pub(crate) fn stamp_with(&mut self, stamp: Stamp) {
        self.inner.stamp = Some(stamp);
    }

    /// Whether writes are stamped: the target may be a SkyS3 peer.
    pub(crate) fn stamped(&self) -> bool {
        self.inner.stamp.is_some()
    }
}

impl<S: fmt::Debug> fmt::Debug for Paced<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Paced")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// A store whose writes that publish or delete a version (`PutObject`,
/// `DeleteObject`, `CopyObject`, `CompleteMultipartUpload`) carry an
/// apply-by time, stamped as each is sent, if it has a [`Stamp`]: a SkyS3
/// peer refuses to sequence them later (§7.8). Other requests change no
/// key and pass as they are.
#[derive(Debug)]
pub(crate) struct Stamping<S> {
    inner: Arc<S>,
    stamp: Option<Stamp>,
}

impl<S> Stamping<S> {
    pub(crate) fn new(inner: Arc<S>, stamp: Option<Stamp>) -> Self {
        Self { inner, stamp }
    }

    /// The apply-by time of a write sent now, if writes are stamped.
    fn apply_by_ms(&self) -> Option<u64> {
        self.stamp.as_ref().map(Stamp::apply_by_ms)
    }
}

impl<S: ObjectStore> ObjectStore for Stamping<S> {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        let apply_by_ms = self.apply_by_ms();
        let request = PutObject {
            apply_by_ms,
            ..request
        };
        self.inner.put_object(request).await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.inner.get_object(request).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.inner.head_object(request).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        let apply_by_ms = self.apply_by_ms();
        let request = DeleteObject {
            apply_by_ms,
            ..request
        };
        self.inner.delete_object(request).await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.inner.list_objects_v2(request).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        let apply_by_ms = self.apply_by_ms();
        let request = CopyObject {
            apply_by_ms,
            ..request
        };
        self.inner.copy_object(request).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.inner.create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.inner.upload_part(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        let apply_by_ms = self.apply_by_ms();
        let request = CompleteMultipartUpload {
            apply_by_ms,
            ..request
        };
        self.inner.complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.inner.abort_multipart_upload(request).await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.inner.list_parts(request).await
    }
}

impl<S: ObjectStore> ObjectStore for Paced<S> {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        self.window.send(self.inner.put_object(request)).await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.window.send(self.inner.get_object(request)).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.window.send(self.inner.head_object(request)).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        self.window.send(self.inner.delete_object(request)).await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.window.send(self.inner.list_objects_v2(request)).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        self.window.send(self.inner.copy_object(request)).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.window
            .send(self.inner.create_multipart_upload(request))
            .await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.window.send(self.inner.upload_part(request)).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        self.window
            .send(self.inner.complete_multipart_upload(request))
            .await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.window
            .send(self.inner.abort_multipart_upload(request))
            .await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.window.send(self.inner.list_parts(request)).await
    }
}

/// A bug seeded into adaptive concurrency, for a simulation to catch (the
/// `test-util` feature exports [`seed_concurrency_bug`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConcurrencyBug {
    /// No bug.
    #[default]
    None,
    /// Throttles do not shrink the window.
    IgnoresThrottles,
    /// Requests do not wait for the in-flight byte bound.
    IgnoresInflightBytes,
}

#[cfg(feature = "test-util")]
pub use hooks::seed_concurrency_bug;

pub(crate) mod hooks {
    use std::cell::Cell;

    use super::ConcurrencyBug;

    thread_local! {
        static BUG: Cell<ConcurrencyBug> = const { Cell::new(ConcurrencyBug::None) };
    }

    /// Seeds `bug` into the adaptive concurrency of every target this
    /// thread's flushers send to. A deterministic simulation runs every
    /// node on its test's thread, so other tests run the real code.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn seed_concurrency_bug(bug: ConcurrencyBug) {
        BUG.with(|seeded| seeded.set(bug));
    }

    /// The bug seeded on this thread.
    pub(crate) fn bug() -> ConcurrencyBug {
        BUG.with(Cell::get)
    }
}

#[cfg(test)]
mod tests;
