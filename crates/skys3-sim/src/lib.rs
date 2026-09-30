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
//!
//! The cluster harness and history checkers (plan M2-03) are added later as
//! modules beside these.
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
pub mod s3;

pub use runner::{NodeClock, Runner, SeedSet, SimContext};
pub use s3::SimS3;
