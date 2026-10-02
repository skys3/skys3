//! A node's pool of peer connections (design §7.8, *Topology*).
//!
//! The flushers of every shard a node hosts share one pool. Connections
//! are pooled per destination, and a destination's connection count
//! adapts the way REST flush concurrency does (§7.7): it grows by one
//! connection per attached shard while throughput rises and the round
//! trip stays near its base, and it halves when the round trip rises or a
//! connection is lost. It never exceeds `peer_connections_per_shard` for
//! each shard flushing to the destination, nor drops below one. New
//! streams go to an idle connection, else to a new one while the count
//! allows, else to the connection with the fewest open streams.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::endpoint::{ConnectError, Destination, PeerConnection, PeerEndpoint};
use crate::stream::{MessageStream, StreamCount, StreamError};

/// A round trip this many times its base, plus [`LATENCY_SLACK`], counts
/// as rising latency.
pub const LATENCY_TOLERANCE: f64 = 1.5;

/// The round-trip growth that never counts as rising latency, so jitter on
/// a sub-millisecond path does not read as congestion.
pub const LATENCY_SLACK: Duration = Duration::from_millis(5);

/// Throughput must rise by this factor from one sample to the next for the
/// limit to keep growing.
pub const GROWTH_THRESHOLD: f64 = 1.05;

/// What a destination's connections did since the previous sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    /// Bytes sent per second.
    pub throughput: f64,
    /// The mean smoothed round trip.
    pub rtt: Duration,
    /// The lowest round trip seen: the path's base.
    pub base_rtt: Duration,
    /// Whether the limit held back demand: every connection allowed was
    /// open and busy.
    pub saturated: bool,
}

/// An additive-increase, multiplicative-decrease limit on concurrency, as
/// REST flush concurrency adapts (§7.7).
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveLimit {
    current: u32,
    max: u32,
    step: u32,
    last_throughput: Option<f64>,
}

impl AdaptiveLimit {
    /// A limit that starts at one and grows by `step` up to `max`.
    #[must_use]
    pub fn new(max: u32, step: u32) -> Self {
        Self {
            current: 1,
            max: max.max(1),
            step: step.max(1),
            last_throughput: None,
        }
    }

    /// The limit now.
    #[must_use]
    pub fn current(&self) -> u32 {
        self.current
    }

    /// The ceiling.
    #[must_use]
    pub fn max(&self) -> u32 {
        self.max
    }

    /// Changes the ceiling and the step, lowering the limit to the new
    /// ceiling if needed.
    pub fn set_ceiling(&mut self, max: u32, step: u32) {
        self.max = max.max(1);
        self.step = step.max(1);
        self.current = self.current.min(self.max);
    }

    /// Adapts to a sample: halves on rising latency, grows by the step
    /// while saturated and throughput rises, and otherwise holds.
    pub fn on_sample(&mut self, sample: &Sample) -> u32 {
        let rising_latency = sample.base_rtt > Duration::ZERO
            && sample.rtt.as_secs_f64()
                > sample.base_rtt.as_secs_f64() * LATENCY_TOLERANCE + LATENCY_SLACK.as_secs_f64();
        let throughput_rose = self
            .last_throughput
            .is_none_or(|last| sample.throughput >= last * GROWTH_THRESHOLD);
        self.last_throughput = Some(sample.throughput);
        if rising_latency {
            self.back_off();
        } else if sample.saturated && throughput_rose {
            self.current = self.current.saturating_add(self.step).min(self.max);
        }
        self.current
    }

    /// Halves the limit after an overload: a lost connection, or a
    /// destination that says it is unavailable.
    pub fn on_overload(&mut self) -> u32 {
        self.back_off();
        self.current
    }

    fn back_off(&mut self) {
        self.current = (self.current / 2).max(1);
        // Growth resumes against the throughput after the back-off.
        self.last_throughput = None;
    }
}

/// Why a pooled stream could not be opened.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PoolError {
    /// A new connection could not be set up.
    #[error(transparent)]
    Connect(#[from] ConnectError),
    /// The stream could not be opened on the connection chosen.
    #[error(transparent)]
    Stream(#[from] StreamError),
}

/// A destination's place in a pool, as [`ConnectionPool::stats`] reports
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStats {
    /// The shards attached to the destination.
    pub shards: u32,
    /// The connection limit now.
    pub limit: u32,
    /// The ceiling: `peer_connections_per_shard` times the shards.
    pub max: u32,
    /// The connections open.
    pub connections: usize,
    /// The streams open, or being opened, on each connection.
    pub streams: Vec<usize>,
}

/// The peer connections of every shard a node hosts, pooled per
/// destination. Cloning it gives another handle to the same pool.
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug)]
struct PoolInner {
    endpoint: PeerEndpoint,
    per_shard: u32,
    destinations: Mutex<HashMap<Destination, Arc<DestinationPool>>>,
}

#[derive(Debug)]
struct DestinationPool {
    state: Mutex<PoolState>,
    connected: Notify,
}

#[derive(Debug)]
struct PoolState {
    shards: u32,
    limit: AdaptiveLimit,
    connections: Vec<Pooled>,
    connecting: u32,
    overloaded: bool,
    sampled_at: Instant,
}

#[derive(Debug)]
struct Pooled {
    connection: PeerConnection,
    /// UDP bytes sent at the previous sample.
    sent: u64,
}

impl PoolState {
    /// Drops closed connections, noting a lost one as an overload.
    fn prune(&mut self) {
        self.connections.retain(|pooled| {
            let Some(reason) = pooled.connection.close_reason() else {
                return true;
            };
            if !matches!(
                reason,
                quinn::ConnectionError::LocallyClosed
                    | quinn::ConnectionError::ApplicationClosed(_)
            ) {
                self.overloaded = true;
            }
            false
        });
    }

    fn live(&self) -> u32 {
        u32::try_from(self.connections.len()).unwrap_or(u32::MAX)
    }

    /// Closes idle connections past the limit, newest first.
    fn trim(&mut self) {
        let limit = self.limit.current() as usize;
        while self.connections.len() > limit {
            let Some(index) = self
                .connections
                .iter()
                .rposition(|pooled| pooled.connection.open_streams() == 0)
            else {
                return;
            };
            self.connections.remove(index).connection.close();
        }
    }
}

/// What a caller of [`ShardLease::open_stream`] does next. Each choice
/// holds the reservation it made under the state lock, so concurrent
/// callers see it, and dropping it, as a failed or cancelled call does,
/// releases it.
enum Choice {
    /// Open a stream on this connection, whose slot is reserved.
    Use(PeerConnection, StreamCount),
    /// Open a new connection in the reserved slot.
    Connect(ConnectSlot),
    /// Wait for a connection being opened by another caller.
    Wait,
}

/// A reserved slot for a connection being opened. Dropping it frees the
/// slot and wakes the callers waiting for a connection, whether the
/// connect succeeded, failed, or was cancelled.
struct ConnectSlot(Arc<DestinationPool>);

impl ConnectSlot {
    /// Reserves a slot; the caller holds the state lock as `state`.
    fn reserve(pool: &Arc<DestinationPool>, state: &mut PoolState) -> Self {
        state.connecting += 1;
        Self(pool.clone())
    }
}

impl Drop for ConnectSlot {
    fn drop(&mut self) {
        lock(&self.0.state).connecting -= 1;
        self.0.connected.notify_waiters();
    }
}

impl ConnectionPool {
    /// A pool that connects through `endpoint`, with at most
    /// `connections_per_shard` (`peer_connections_per_shard`) connections
    /// to a destination for each shard attached to it.
    #[must_use]
    pub fn new(endpoint: PeerEndpoint, connections_per_shard: u32) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                endpoint,
                per_shard: connections_per_shard.max(1),
                destinations: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Attaches one shard's flusher to `destination`, raising the
    /// destination's ceiling by `peer_connections_per_shard` until the
    /// lease is dropped.
    #[must_use]
    pub fn attach(&self, destination: Destination) -> ShardLease {
        let mut destinations = lock(&self.inner.destinations);
        let pool = destinations
            .entry(destination.clone())
            .or_insert_with(|| {
                Arc::new(DestinationPool {
                    state: Mutex::new(PoolState {
                        shards: 0,
                        limit: AdaptiveLimit::new(self.inner.per_shard, 1),
                        connections: Vec::new(),
                        connecting: 0,
                        overloaded: false,
                        sampled_at: Instant::now(),
                    }),
                    connected: Notify::new(),
                })
            })
            .clone();
        {
            let mut state = lock(&pool.state);
            state.shards += 1;
            let shards = state.shards;
            state
                .limit
                .set_ceiling(self.inner.per_shard.saturating_mul(shards), shards);
        }
        ShardLease {
            pool: self.clone(),
            destination,
            destination_pool: pool,
        }
    }

    /// Where `destination` stands, or `None` if no shard is attached to it.
    #[must_use]
    pub fn stats(&self, destination: &Destination) -> Option<PoolStats> {
        let pool = lock(&self.inner.destinations).get(destination)?.clone();
        let state = lock(&pool.state);
        Some(PoolStats {
            shards: state.shards,
            limit: state.limit.current(),
            max: state.limit.max(),
            connections: state.connections.len(),
            streams: state
                .connections
                .iter()
                .map(|pooled| pooled.connection.open_streams())
                .collect(),
        })
    }

    /// Samples every destination's connections and adapts its limit,
    /// closing idle connections past it. The flusher calls this on a
    /// timer, a few round trips apart.
    pub fn adapt(&self) {
        let pools: Vec<_> = lock(&self.inner.destinations).values().cloned().collect();
        let now = Instant::now();
        for pool in pools {
            let mut state = lock(&pool.state);
            state.prune();
            let elapsed = now.saturating_duration_since(state.sampled_at);
            state.sampled_at = now;
            let mut sent = 0;
            let mut rtt = Duration::ZERO;
            let mut base_rtt = Duration::MAX;
            for pooled in &mut state.connections {
                let stats = pooled.connection.stats();
                sent += stats.udp_tx.bytes.saturating_sub(pooled.sent);
                pooled.sent = stats.udp_tx.bytes;
                rtt += stats.path.rtt;
                base_rtt = base_rtt.min(stats.path.min_rtt);
            }
            if std::mem::take(&mut state.overloaded) {
                state.limit.on_overload();
            } else if let Ok(live @ 1..) = u32::try_from(state.connections.len()) {
                let busy = state
                    .connections
                    .iter()
                    .all(|pooled| pooled.connection.open_streams() > 0);
                let sample = Sample {
                    throughput: sent as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
                    rtt: rtt / live,
                    base_rtt,
                    saturated: busy && live + state.connecting >= state.limit.current(),
                };
                state.limit.on_sample(&sample);
            }
            state.trim();
        }
    }
}

/// One shard flusher's attachment to a destination. Dropping it lowers the
/// destination's ceiling, and the last lease of a destination releases its
/// connections, which close once their streams are done.
#[derive(Debug)]
pub struct ShardLease {
    pool: ConnectionPool,
    destination: Destination,
    destination_pool: Arc<DestinationPool>,
}

impl ShardLease {
    /// The destination.
    #[must_use]
    pub fn destination(&self) -> &Destination {
        &self.destination
    }

    /// Opens a stream to the destination for one object or batch, on an
    /// idle connection, a new one while the limit allows, or the
    /// connection with the fewest open streams.
    ///
    /// # Errors
    ///
    /// [`PoolError::Connect`] if a new connection could not be set up, and
    /// [`PoolError::Stream`] if the stream could not be opened.
    pub async fn open_stream(&self) -> Result<MessageStream, PoolError> {
        let pool = &self.destination_pool;
        loop {
            let connected = pool.connected.notified();
            let choice = {
                let mut state = lock(&pool.state);
                state.prune();
                let idle = state
                    .connections
                    .iter()
                    .find(|pooled| pooled.connection.open_streams() == 0);
                let least_busy = state
                    .connections
                    .iter()
                    .min_by_key(|pooled| pooled.connection.open_streams());
                if let Some(idle) = idle {
                    let connection = idle.connection.clone();
                    let reservation = connection.reserve_stream();
                    Choice::Use(connection, reservation)
                } else if state.live() + state.connecting < state.limit.current() {
                    Choice::Connect(ConnectSlot::reserve(pool, &mut state))
                } else if let Some(least_busy) = least_busy {
                    let connection = least_busy.connection.clone();
                    let reservation = connection.reserve_stream();
                    Choice::Use(connection, reservation)
                } else {
                    Choice::Wait
                }
            };
            match choice {
                Choice::Use(connection, reservation) => {
                    return Ok(connection.open_reserved(reservation).await?);
                }
                Choice::Connect(slot) => {
                    let (connection, reservation) = self.connect(slot).await?;
                    return Ok(connection.open_reserved(reservation).await?);
                }
                Choice::Wait => connected.await,
            }
        }
    }

    /// Reports that the destination is overloaded, for example an
    /// `APPLIED` that says `unavailable`; the next [`ConnectionPool::adapt`]
    /// halves the limit.
    pub fn report_overload(&self) {
        lock(&self.destination_pool.state).overloaded = true;
    }

    /// Opens a connection in a reserved slot, and adds it to the pool with
    /// a stream reserved for the caller, so that the callers `slot` wakes
    /// do not take it for an idle connection.
    async fn connect(
        &self,
        slot: ConnectSlot,
    ) -> Result<(PeerConnection, StreamCount), ConnectError> {
        let connection = self.pool.inner.endpoint.connect(&self.destination).await?;
        let reservation = connection.reserve_stream();
        lock(&self.destination_pool.state).connections.push(Pooled {
            sent: connection.stats().udp_tx.bytes,
            connection: connection.clone(),
        });
        drop(slot);
        Ok((connection, reservation))
    }
}

impl Drop for ShardLease {
    fn drop(&mut self) {
        let mut destinations = lock(&self.pool.inner.destinations);
        let mut state = lock(&self.destination_pool.state);
        state.shards -= 1;
        let shards = state.shards;
        if shards == 0 {
            drop(state);
            destinations.remove(&self.destination);
        } else {
            let max = self.pool.inner.per_shard.saturating_mul(shards);
            state.limit.set_ceiling(max, shards);
            state.trim();
        }
    }
}

/// Locks `mutex`. The pool's state stays consistent between statements,
/// so a panic elsewhere while it was held does not invalidate it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(throughput: f64, rtt_ms: u64, saturated: bool) -> Sample {
        Sample {
            throughput,
            rtt: Duration::from_millis(rtt_ms),
            base_rtt: Duration::from_millis(100),
            saturated,
        }
    }

    #[test]
    fn grows_additively_while_throughput_rises() {
        let mut limit = AdaptiveLimit::new(10, 2);
        assert_eq!(limit.current(), 1);
        assert_eq!(limit.on_sample(&sample(100.0, 100, true)), 3);
        assert_eq!(limit.on_sample(&sample(200.0, 110, true)), 5);
        // Flat throughput holds the limit.
        assert_eq!(limit.on_sample(&sample(201.0, 110, true)), 5);
        // So does demand the limit does not hold back.
        assert_eq!(limit.on_sample(&sample(400.0, 110, false)), 5);
        assert_eq!(limit.on_sample(&sample(800.0, 110, true)), 7);
        assert_eq!(limit.on_sample(&sample(1600.0, 110, true)), 9);
        assert_eq!(limit.on_sample(&sample(3200.0, 110, true)), 10, "capped");
    }

    #[test]
    fn halves_on_rising_latency_or_overload() {
        let mut limit = AdaptiveLimit::new(64, 8);
        for throughput in [1.0, 2.0, 4.0, 8.0] {
            limit.on_sample(&sample(throughput, 100, true));
        }
        assert_eq!(limit.current(), 33);
        assert_eq!(
            limit.on_sample(&sample(8.0, 155, true)),
            33,
            "within the slack"
        );
        assert_eq!(limit.on_sample(&sample(16.0, 156, true)), 16);
        assert_eq!(limit.on_overload(), 8);
        // After a back-off, the next rising sample grows again.
        assert_eq!(limit.on_sample(&sample(1.0, 100, true)), 16);
        for _ in 0..10 {
            limit.on_overload();
        }
        assert_eq!(limit.current(), 1, "never below one");
        // Without a base round trip, latency cannot be judged.
        let mut fresh = AdaptiveLimit::new(4, 1);
        let unknown_base = Sample {
            base_rtt: Duration::ZERO,
            ..sample(1.0, 500, true)
        };
        assert_eq!(fresh.on_sample(&unknown_base), 2);
    }

    #[test]
    fn a_lower_ceiling_lowers_the_limit() {
        let mut limit = AdaptiveLimit::new(8, 4);
        limit.on_sample(&sample(1.0, 100, true));
        limit.on_sample(&sample(2.0, 100, true));
        assert_eq!(limit.current(), 8);
        limit.set_ceiling(4, 2);
        assert_eq!((limit.current(), limit.max()), (4, 4));
        limit.set_ceiling(0, 0);
        assert_eq!((limit.current(), limit.max()), (1, 1));
    }
}
