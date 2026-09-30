#![forbid(unsafe_code)]
//! Deterministic simulation for SkyS3 (design §16.1).
//!
//! Simulations run the real node code on `turmoil`, with simulated disks and
//! drifting clocks from `skys3-io`. Everything random derives from one seed,
//! so a failing seed replays exactly.
//!
//! - [`runner`]: runs a scenario over a fixed set of seeds and, on failure,
//!   reports the seed and the command that replays it.
//!
//! Later parts of the plan add the simulated S3 store (M0-05), and the
//! cluster harness and history checkers (M2-03), as modules beside the
//! runner.
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

pub mod runner;

pub use runner::{NodeClock, Runner, SeedSet, SimContext};
