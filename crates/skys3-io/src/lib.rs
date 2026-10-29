#![forbid(unsafe_code)]
//! Disk, clock, and blocking-pool abstractions for SkyS3, real and
//! simulated.
//!
//! The storage engine and every timer reach the outside world only through
//! these interfaces, so the same code runs on a node and under deterministic
//! simulation (design §16.1):
//!
//! - [`Disk`] and [`SegmentFile`]: a directory of append-only segment files
//!   with `fdatasync`, directory `fsync`, and reads at an offset.
//!   [`RealDisk`] runs each operation on a dedicated [`BlockingPool`], never
//!   on the Tokio reactor (design §10.4). [`SimDisk`] tracks written versus
//!   synced bytes, loses unsynced bytes and unsynced directory entries on a
//!   simulated crash, and injects sync errors, torn writes, and full disks.
//! - [`Clock`]: a node's monotonic clock. [`MonotonicClock`] follows the
//!   runtime's clock and can drift by a bounded rate, as the lease rules
//!   assume (design §5.4).
//! - [`BlockingPool`]: a fixed set of named threads for blocking work.

pub mod clock;
pub mod disk;
pub mod pool;

pub use clock::{Clock, Drift, MonoTime, MonotonicClock};
pub use disk::{Disk, RealDisk, SegmentFile, SimDisk, SimDiskFaults, SimMount};
pub use pool::{BlockingPool, PoolClosed};
