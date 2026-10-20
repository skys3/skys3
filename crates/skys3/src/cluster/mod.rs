//! The node's cluster services, one file per service.
//!
//! - [`control`]: following the control store (design §6.2), which keeps
//!   the node's copy of bucket bindings and identity configuration current
//!   and reopens a store that failed.
//! - [`peering`]: native replication with peer clusters over QUIC (design
//!   §7.8).

pub(crate) mod control;
pub(crate) mod peering;
