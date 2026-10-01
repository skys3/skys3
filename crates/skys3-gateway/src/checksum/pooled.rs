//! Hashing a stream on a blocking pool (design §15), off the reactor.

use std::future::{Future, poll_fn};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use skys3_io::{BlockingPool, PoolClosed};
use skys3_types::checksum::ChecksumAlgorithm;

use super::hasher::{Digests, Hashers};

/// The bytes a [`PooledHasher`] collects before it hands them to the pool
/// in one job. A job costs a thread handoff, a few microseconds, and
/// hashing this much with MD5 takes about half a millisecond.
pub const HASH_BATCH_BYTES: usize = 256 * 1024;

type Job = Pin<Box<dyn Future<Output = Result<Hashers, PoolClosed>> + Send + Sync>>;

/// Computes digests of a stream of byte chunks on a [`BlockingPool`].
///
/// Chunks are [`Bytes`], so handing one to the pool copies nothing. They
/// are hashed in order, in batches of about [`HASH_BATCH_BYTES`], with at
/// most one batch on the pool at a time: the caller goes on reading while
/// the previous batch is hashed, and waits only when a second batch is
/// full. Memory is therefore bounded by two batches.
///
/// Without a pool, chunks are hashed inline as they arrive, which is for
/// tests and for small bodies, such as XML, that never reach the pool's
/// threshold anyway.
pub struct PooledHasher {
    pool: Option<BlockingPool>,
    state: State,
    pending: Vec<Bytes>,
    pending_len: usize,
}

enum State {
    Idle(Hashers),
    Running(Job),
    /// The pool was shut down, or the digests were taken.
    Closed,
}

impl PooledHasher {
    /// A hasher for `algorithms` that hashes on `pool`, or inline without
    /// one.
    pub fn new(
        algorithms: impl IntoIterator<Item = ChecksumAlgorithm>,
        pool: Option<BlockingPool>,
    ) -> Self {
        Self {
            pool,
            state: State::Idle(Hashers::new(algorithms)),
            pending: Vec::new(),
            pending_len: 0,
        }
    }

    /// Waits until the hasher can take another chunk without holding more
    /// than two batches.
    ///
    /// # Errors
    ///
    /// [`PoolClosed`] if the pool was shut down.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), PoolClosed>> {
        if self.pending_len < HASH_BATCH_BYTES {
            return Poll::Ready(Ok(()));
        }
        ready!(self.poll_idle(cx))?;
        self.start();
        Poll::Ready(Ok(()))
    }

    /// Adds the next chunk. Call it after [`PooledHasher::poll_ready`]
    /// returned `Ready(Ok(()))`.
    pub fn push(&mut self, data: Bytes) {
        if data.is_empty() {
            return;
        }
        if self.pool.is_none() {
            if let State::Idle(hashers) = &mut self.state {
                hashers.update(&data);
            }
            return;
        }
        self.pending_len += data.len();
        self.pending.push(data);
        if self.pending_len >= HASH_BATCH_BYTES && matches!(self.state, State::Idle(_)) {
            self.start();
        }
    }

    /// Waits until every chunk is hashed, and returns the digests. The
    /// hasher is spent afterwards.
    ///
    /// # Errors
    ///
    /// [`PoolClosed`] if the pool was shut down, or if the digests were
    /// already taken.
    pub fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<Digests, PoolClosed>> {
        loop {
            ready!(self.poll_idle(cx))?;
            if self.pending.is_empty() {
                break;
            }
            self.start();
        }
        match mem::replace(&mut self.state, State::Closed) {
            State::Idle(hashers) => Poll::Ready(Ok(hashers.finish())),
            // `poll_idle` returned only with the hashers idle.
            State::Running(_) | State::Closed => Poll::Ready(Err(PoolClosed)),
        }
    }

    /// Adds the next chunk, waiting first if two batches are held.
    ///
    /// # Errors
    ///
    /// [`PoolClosed`] if the pool was shut down.
    pub async fn update(&mut self, data: Bytes) -> Result<(), PoolClosed> {
        poll_fn(|cx| self.poll_ready(cx)).await?;
        self.push(data);
        Ok(())
    }

    /// Waits until every chunk is hashed, and returns the digests.
    ///
    /// # Errors
    ///
    /// [`PoolClosed`] if the pool was shut down.
    pub async fn finish(mut self) -> Result<Digests, PoolClosed> {
        poll_fn(|cx| self.poll_finish(cx)).await
    }

    /// Waits for the batch on the pool, if any.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), PoolClosed>> {
        match &mut self.state {
            State::Idle(_) => Poll::Ready(Ok(())),
            State::Running(job) => {
                let result = ready!(job.as_mut().poll(cx));
                match result {
                    Ok(hashers) => {
                        self.state = State::Idle(hashers);
                        Poll::Ready(Ok(()))
                    }
                    Err(closed) => {
                        self.state = State::Closed;
                        Poll::Ready(Err(closed))
                    }
                }
            }
            State::Closed => Poll::Ready(Err(PoolClosed)),
        }
    }

    /// Hands the pending chunks to the pool. The hashers must be idle.
    fn start(&mut self) {
        let (State::Idle(mut hashers), Some(pool)) =
            (mem::replace(&mut self.state, State::Closed), &self.pool)
        else {
            unreachable!("a batch starts only on a pool, with the hashers idle");
        };
        let batch = mem::take(&mut self.pending);
        self.pending_len = 0;
        self.state = State::Running(Box::pin(pool.run(move || {
            for chunk in &batch {
                hashers.update(chunk);
            }
            hashers
        })));
    }
}

impl std::fmt::Debug for PooledHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledHasher")
            .field("pool", &self.pool)
            .field("pending_len", &self.pending_len)
            .finish_non_exhaustive()
    }
}
