//! Dedicated pools of blocking worker threads.
//!
//! Disk I/O never runs on the Tokio reactor (design §10.4), and not on
//! Tokio's shared `spawn_blocking` pool either: that pool also serves
//! unrelated blocking work and grows without a useful bound, so one slow disk
//! could occupy it. Each [`BlockingPool`] owns a fixed set of named threads.
//! A node runs one pool per disk, so a stalled disk holds up only its own
//! queue, and a separate pool for hashing (design §15).
//!
//! Deterministic simulation (design §16.1) uses [`BlockingPool::inline`]
//! instead: worker threads finish at times no seed controls, so a
//! simulated runtime that waits for one would advance its clock by
//! different amounts on each run.

use std::any::Any;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;

use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A fixed-size pool of named threads that run blocking jobs for async
/// callers.
///
/// Cloning a pool returns another handle to the same threads. Jobs run in
/// submission order, up to one per thread at a time. Submission never
/// blocks, so it is safe on the reactor: the queue is unbounded, and callers
/// bound their own outstanding work (group commit does, for example). The
/// threads exit once the pool is shut down or every handle is dropped, after
/// finishing the jobs already queued.
#[derive(Clone)]
pub struct BlockingPool {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    threads: usize,
    /// `None` once the pool is shut down.
    sender: Mutex<Option<Sender<Job>>>,
    /// Whether jobs run on the caller's thread instead (no workers).
    inline: bool,
}

impl BlockingPool {
    /// Starts a pool of `threads` threads named `<name>-0`, `<name>-1`, and
    /// so on.
    ///
    /// # Errors
    ///
    /// Returns the operating system's error if a thread cannot be spawned.
    /// Threads already started exit.
    pub fn new(name: &str, threads: NonZeroUsize) -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..threads.get() {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("{name}-{index}"))
                .spawn(move || worker(&receiver))?;
        }
        Ok(BlockingPool {
            inner: Arc::new(Inner {
                name: name.to_owned(),
                threads: threads.get(),
                sender: Mutex::new(Some(sender)),
                inline: false,
            }),
        })
    }

    /// Returns a pool without threads that runs each job on the caller's
    /// thread, at once, when it is queued: for deterministic simulation,
    /// where only the simulation may decide how long a job takes. Jobs must
    /// not wait for the caller's runtime. [`BlockingPool::threads`] is zero.
    pub fn inline(name: &str) -> Self {
        let (sender, _) = mpsc::channel::<Job>();
        BlockingPool {
            inner: Arc::new(Inner {
                name: name.to_owned(),
                threads: 0,
                sender: Mutex::new(Some(sender)),
                inline: true,
            }),
        }
    }

    /// Returns the pool's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the number of worker threads, zero for an inline pool.
    pub fn threads(&self) -> usize {
        self.inner.threads
    }

    /// Queues `job` and returns a future that resolves to its result.
    ///
    /// The job is queued when this method is called, not when the future is
    /// first polled, and it runs even if the future is dropped.
    ///
    /// # Errors
    ///
    /// The future resolves to [`PoolClosed`] if the pool was shut down before
    /// the job was queued.
    ///
    /// # Panics
    ///
    /// If the job panics, the panic resumes in the task that awaits the
    /// future. The worker thread survives.
    pub fn run<F, T>(&self, job: F) -> impl Future<Output = Result<T, PoolClosed>> + Send + 'static
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel::<thread::Result<T>>();
        let wrapped: Job = Box::new(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(job));
            // The caller may have stopped waiting; the result is then unused.
            let _ = result_tx.send(result);
        });
        let queued = {
            let sender = self
                .inner
                .sender
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match sender.as_ref() {
                None => false,
                Some(_) if self.inner.inline => {
                    // Not under the lock: the job may queue more jobs.
                    drop(sender);
                    wrapped();
                    true
                }
                Some(sender) => sender.send(wrapped).is_ok(),
            }
        };
        async move {
            if !queued {
                return Err(PoolClosed);
            }
            match result_rx.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(payload)) => resume_panic(payload),
                // Unreachable: a worker always sends the job's result.
                Err(_) => Err(PoolClosed),
            }
        }
    }

    /// Stops accepting jobs. Jobs already queued still run, and then the
    /// threads exit. Does not wait for them.
    pub fn shutdown(&self) {
        self.inner
            .sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    /// Returns whether [`BlockingPool::shutdown`] was called.
    pub fn is_shut_down(&self) -> bool {
        self.inner
            .sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }
}

impl fmt::Debug for BlockingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingPool")
            .field("name", &self.inner.name)
            .field("threads", &self.inner.threads)
            .field("shut_down", &self.is_shut_down())
            .finish()
    }
}

fn worker(receiver: &Mutex<Receiver<Job>>) {
    loop {
        // Hold the lock only while waiting for a job, not while running it.
        let job = receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv();
        match job {
            Ok(job) => job(),
            // Every sender is gone: the pool was shut down or dropped.
            Err(_) => return,
        }
    }
}

fn resume_panic<T>(payload: Box<dyn Any + Send>) -> T {
    panic::resume_unwind(payload)
}

/// The error returned for a job submitted to a pool that was shut down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolClosed;

impl fmt::Display for PoolClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("blocking pool is shut down")
    }
}

impl std::error::Error for PoolClosed {}

impl From<PoolClosed> for io::Error {
    fn from(error: PoolClosed) -> io::Error {
        io::Error::other(error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Barrier;

    use super::*;

    fn pool(threads: usize) -> BlockingPool {
        BlockingPool::new("test-pool", NonZeroUsize::new(threads).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn runs_jobs_on_named_pool_threads() {
        let pool = pool(2);
        assert_eq!(pool.name(), "test-pool");
        assert_eq!(pool.threads(), 2);
        let name = pool
            .run(|| thread::current().name().map(str::to_owned))
            .await
            .unwrap()
            .unwrap();
        assert!(name.starts_with("test-pool-"), "{name}");
        assert_ne!(Some(name.as_str()), thread::current().name());
    }

    #[tokio::test]
    async fn runs_jobs_concurrently_on_all_threads() {
        let pool = pool(3);
        let barrier = Arc::new(Barrier::new(3));
        let jobs: Vec<_> = (0..3)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                pool.run(move || {
                    // Deadlocks unless all three jobs run at once.
                    barrier.wait();
                    thread::current().name().unwrap().to_owned()
                })
            })
            .collect();
        let mut names = HashSet::new();
        for job in jobs {
            names.insert(job.await.unwrap());
        }
        assert_eq!(names.len(), 3);
    }

    #[tokio::test]
    async fn job_runs_even_if_the_future_is_dropped() {
        let pool = pool(1);
        let (tx, rx) = std::sync::mpsc::channel();
        drop(pool.run(move || tx.send(42).unwrap()));
        assert_eq!(pool.run(|| ()).await, Ok(()));
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[tokio::test]
    async fn panics_resume_in_the_caller_and_the_worker_survives() {
        let pool = pool(1);
        let job = pool.run(|| -> u32 { panic!("job failed") });
        let caught = tokio::spawn(job).await.unwrap_err();
        assert!(caught.is_panic());
        assert_eq!(pool.run(|| 5).await, Ok(5));
    }

    #[tokio::test]
    async fn inline_pools_run_jobs_on_the_caller_when_queued() {
        let pool = BlockingPool::inline("sim");
        assert_eq!((pool.name(), pool.threads()), ("sim", 0));
        let caller = thread::current().id();
        let (tx, rx) = std::sync::mpsc::channel();
        let job = pool.run(move || {
            tx.send(()).unwrap();
            thread::current().id()
        });
        // The job ran before its future was polled.
        rx.try_recv().unwrap();
        assert_eq!(job.await, Ok(caller));
        let inner = pool.clone();
        assert_eq!(pool.run(move || inner.threads()).await, Ok(0));

        let caught = tokio::spawn(pool.run(|| -> u32 { panic!("job failed") }))
            .await
            .unwrap_err();
        assert!(caught.is_panic());
        pool.shutdown();
        assert_eq!(pool.run(|| 2).await, Err(PoolClosed));
    }

    #[tokio::test]
    async fn shutdown_rejects_new_jobs() {
        let pool = pool(1);
        assert!(!pool.is_shut_down());
        let queued = pool.run(|| 1);
        pool.clone().shutdown();
        assert!(pool.is_shut_down());
        assert_eq!(queued.await, Ok(1));
        assert_eq!(pool.run(|| 2).await, Err(PoolClosed));

        let error = io::Error::from(PoolClosed);
        assert_eq!(error.to_string(), "blocking pool is shut down");
        assert!(format!("{pool:?}").contains("shut_down: true"));
    }
}
