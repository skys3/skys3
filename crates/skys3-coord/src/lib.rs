#![forbid(unsafe_code)]
//! The SkyS3 coordinator (design §6.7): the node that makes placement
//! decisions, elected by a lease in the control store, and the path every
//! one of its changes takes to the control store and to the nodes
//! (design §6.2).
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! - [`Elector`]: contends for `coordinator.lease` and renews it every
//!   `coordinator_lease / 3` with `If-Match`. A candidate takes over only
//!   after observing the same lease version for longer than
//!   `coordinator_lease × (1+ρ)` on its own monotonic clock. Its
//!   [`Leadership`] says whether the node may act as coordinator.
//! - [`ChangeSet`] and [`apply`]: a change is a sequence of
//!   compare-and-swaps, and every applied change increments the
//!   generation in `cluster.json`. Two nodes that both believe they are
//!   coordinator can only compete: each write is conditional on the
//!   version its planner read.
//! - [`Pusher`] and [`ControlHints`]: the generation that announces a
//!   change is pushed to every node over the intra-cluster transport as a
//!   [`ControlChanged`] frame. Nodes that miss a push learn the generation
//!   from their change streams (a native watch or polling).
//! - [`Coordinator`]: the work loop that, while the node holds the lease,
//!   asks a [`Placement`] what to change. Placement itself is the work of
//!   later tasks; [`NoPlacement`] changes nothing.
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use skys3_control::{MemoryControlStore, ProposalIds};
//! use skys3_coord::{Elector, LeaseConfig, Leadership};
//! use skys3_io::{Clock, MonotonicClock};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build()?.block_on(async {
//! let store = MemoryControlStore::new();
//! let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
//! let config = LeaseConfig::new(Duration::from_secs(10), 0.01)?;
//! let mut elector = Elector::new(
//!     store,
//!     "node-1".parse()?,
//!     Arc::clone(&clock),
//!     config,
//!     ProposalIds::seeded(1),
//! );
//! // Nobody holds the lease yet, so the first round takes it.
//! let renew_at = elector.round().await;
//! assert!(elector.leadership().is_coordinator_at(clock.now()));
//! assert!(matches!(elector.leadership(), Leadership::Coordinator { .. }));
//! assert!(renew_at <= clock.now().saturating_add(config.renew_interval()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod change;
mod coordinator;
mod lease;
mod push;

pub use change::{Applied, ChangeError, ChangeSet, Write, apply};
pub use coordinator::{Coordinator, CoordinatorConfig, NoPlacement, Placement};
pub use lease::{Elector, Leadership, LeaseConfig, LeaseConfigError};
pub use push::{
    Announce, ControlChanged, ControlHints, HintError, PUSH_IDLE_TIMEOUT, PushError, Pushed,
    Pusher,
};
