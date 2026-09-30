//! Simulation scenarios for `skys3-sim`'s own components, run by CI's
//! simulation job with a larger seed set.
//!
//! - [`disk`]: the simulated disk and drifting clocks under crashes.
//! - [`s3`]: the simulated S3 store as a control store and a remote target,
//!   under delays, server errors, throttling, lost requests and responses,
//!   and outages.

mod disk;
mod s3;
