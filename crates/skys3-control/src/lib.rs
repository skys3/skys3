#![forbid(unsafe_code)]
//! The SkyS3 control store: small linearizable registers that hold the
//! cluster's control state (design §6.1, §6.2).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - [`ControlStore`]: the backend interface, `get`, `put_if`,
//!   `delete_if`, `list`, and `changes`. Backends answer each request once; everything above them
//!   is shared.
//! - [`RegisterKey`], [`KeyPrefix`], [`TypedKey`]: the register layout
//!   (`cluster.json`, `coordinator.lease`, `nodes/`, `buckets/`, `shards/`,
//!   `identity/`) as typed paths.
//! - [`propose`]: every register write, with retries, `409` conflicts, and
//!   the lost-response rule: after an unanswered write whose retry fails
//!   its precondition, re-read and look for one's own `proposal_id`.
//!   [`read`] and [`propose_document`] read and write the typed documents
//!   of `skys3-types`, and [`propose_delete`] deletes a register under the
//!   same rules.
//! - [`bootstrap`] and [`bump_generation`]: creating `cluster.json` with
//!   `If-None-Match: *`, and the generation counter that announces
//!   changes.
//! - [`ChangeStream`] and [`ChangeFeed`]: change delivery driven by the
//!   generation, from in-process notification or polling.
//! - Backends: [`MemoryControlStore`] for tests and simulation,
//!   [`FileControlStore`] for single-node development, and
//!   [`S3ControlStore`] over any `skys3-remote` object store, AWS S3 and
//!   S3-compatible providers, and [`EtcdControlStore`] over an etcd v3
//!   cluster, the production default.
//! - [`ControlProbe`]: the startup probe that refuses a store whose
//!   conditional writes, conditional deletes, or reads after writes cannot
//!   be trusted.
//!
//! With the `test-util` feature, `faults` injects lost requests and
//! responses, late requests, conflicts, and outages into any backend, and
//! `conformance` is the suite every backend must pass.
//!
//! ```
//! use skys3_control::{Bootstrap, MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
//! use skys3_types::ClusterId;
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build()?.block_on(async {
//! let store = MemoryControlStore::new();
//! let cluster = ClusterId::new("skys3-prod-a")?;
//! let mut ids = ProposalIds::seeded(7);
//! let policy = RetryPolicy::default();
//! let first = bootstrap(&store, &cluster, ids.next_id(), &policy).await?;
//! let second = bootstrap(&store, &cluster, ids.next_id(), &policy).await?;
//! assert!(matches!(first, Bootstrap::Created(_)));
//! assert_eq!(second, Bootstrap::Existing(first.cluster().clone()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod cluster;
#[cfg(feature = "test-util")]
pub mod conformance;
mod etcd;
#[cfg(any(test, feature = "test-util"))]
pub mod faults;
mod feed;
mod file;
#[doc(hidden)]
pub mod fuzzing;
mod key;
mod memory;
mod probe;
mod propose;
mod s3;
mod store;

pub use cluster::{Bootstrap, FIRST_GENERATION, bootstrap, bump_generation, read_cluster};
pub use etcd::{EtcdConfigError, EtcdControlStore, EtcdStoreConfig};
pub use feed::ChangeFeed;
pub use file::{FileControlStore, FileStoreConfig};
pub use key::{KeyError, KeyPrefix, RegisterKey, RegisterKind, TypedKey};
pub use memory::MemoryControlStore;
pub use probe::{ControlProbe, ProbeError, ProbeFailure};
pub use propose::{
    DeletionOutcome, ProposalIds, ProposalOutcome, RetryPolicy, get_with_retries, proposal_id_of,
    propose, propose_delete, propose_document, read, read_with_retries,
};
pub use s3::{S3Changes, S3ControlStore, S3StoreConfig};
pub use store::{
    Change, ChangeStream, ControlError, ControlStore, DeleteOutcome, Expected, PutOutcome, Version,
    Versioned,
};
