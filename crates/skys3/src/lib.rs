#![forbid(unsafe_code)]
#![doc = include_str!("../../../README.md")]
//!
//! # The node
//!
//! This crate is the `skys3` binary and the library it is built from:
//! [`Node`] starts a node from a validated configuration, recovers it, and
//! serves it until it shuts down (design §3, §6.2, §10). The binary reads
//! the configuration file, sets up logging, and stops the node gracefully
//! on `SIGTERM` or `SIGINT`.
//!
//! - [`datadir`]: the data directory, the node's identity, its disks, and
//!   fencing of a disk an I/O error took out of service.
//! - [`control`]: the node's durable copy of bucket bindings and identity
//!   configuration, and the control store served from it while the store
//!   does not answer.
//! - [`storage`]: recovery of the storage engine, the part of startup the
//!   cluster simulation shares with the node.
//! - [`sessions`]: STS session records in the internal system bucket.
//! - [`admin`]: the admin API under `/v1/`: health and bucket status.
//! - [`tls`]: the gateway's TLS configuration.

pub mod admin;
mod admission;
pub mod control;
pub mod datadir;
mod node;
mod remote;
pub mod sessions;
pub mod storage;
pub mod tls;

pub use node::{Node, NodeError, StartError, log_config};
