#![forbid(unsafe_code)]
//! Deterministic simulation for SkyS3 (design §16.1).
//!
//! Simulations run the real node code on `turmoil`, with simulated disks and
//! drifting clocks from `skys3-io`. Everything random derives from one seed,
//! so a failing seed replays exactly.
//!
//! - [`runner`]: runs a scenario over a fixed set of seeds and, on failure,
//!   reports the seed and the command that replays it.
//! - [`s3`]: the simulated S3 store, which implements `skys3-remote`'s
//!   `ObjectStore` for remote targets and the S3 control store, with
//!   conditional writes, provider profiles, and seeded faults.
//! - [`history`] and [`check`]: histories of client operations on keys,
//!   and the checkers that judge them: per-key linearizability, and that
//!   every acknowledged write survives.
//!
//! The cluster harness, which runs several nodes in one simulation, is the
//! `skys3-cluster-sim` crate: it needs the node's crates, which use this one
//! in their own tests.
//!
//! # Conventions
//!
//! Scenario tests live in an integration test target named `simulation`
//! (`tests/simulation.rs` or `tests/simulation/main.rs`) in the crate whose
//! code they exercise, and run their scenarios through a [`Runner`]. CI's
//! simulation job runs every such target with a larger fixed seed set:
//!
//! ```text
//! SKYS3_SIM_SEEDS=256 cargo test --workspace --all-features --test simulation
//! ```
//!
//! A scenario whose seeds are much slower than most declares its cost with
//! [`Runner::with_cost`], which divides that count. The nightly job runs the
//! same targets with random seeds (`SKYS3_SIM_FIRST_SEED`) and longer runs
//! (`SKYS3_SIM_SCALE`). A seed that fails there becomes a regression test
//! that runs it with [`Runner::with_seeds`].
//!
//! # Example
//!
//! ```
//! use skys3_sim::Runner;
//!
//! Runner::new().run(|context| {
//!     let disk = context.disk();
//!     let mut sim = context.builder().build();
//!     sim.client("node", async move {
//!         let _mount = disk.mount();
//!         Ok(())
//!     });
//!     sim.run()
//! });
//! ```

pub mod check;
pub mod history;
pub mod runner;
pub mod s3;

pub use runner::{NodeClock, Runner, SeedSet, SimContext};
pub use s3::SimS3;
