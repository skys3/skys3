//! `[peering]`: the native QUIC transport between SkyS3 clusters (§7.8).

use std::net::SocketAddr;
use std::time::Duration;

use serde::Deserialize;
use skys3_types::limits::MAX_RECORD_PAYLOAD_LEN;

use crate::error::Checker;
use crate::storage::{KIB, MIB, TIB};

/// The congestion controller of peer connections (§7.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CongestionControl {
    /// Cubic, the default.
    #[default]
    Cubic,
    /// NewReno.
    NewReno,
    /// BBR, which Quinn marks experimental.
    Bbr,
}

/// `[peering]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PeeringConfig {
    /// `quic_listen`: the UDP address peer clusters connect to.
    pub quic_listen: SocketAddr,
    /// `congestion_control`.
    pub congestion_control: CongestionControl,
    /// `peer_frame_bytes`: the size of a `DATA` frame, and the largest
    /// object sent in a `BATCH`.
    pub peer_frame_bytes: u64,
    /// `peer_connect_timeout_ms`: how long `auto` transport waits for a
    /// QUIC handshake before it falls back to S3 REST.
    pub peer_connect_timeout_ms: u64,
    /// `peer_connections_per_shard`: the upper bound of a shard primary's
    /// peer connections; mirrors `flush_max_concurrency_per_shard`.
    pub peer_connections_per_shard: u32,
    /// `peer_max_inflight_bytes`: the cap on stream and connection windows.
    pub peer_max_inflight_bytes: u64,
    /// `peer_staging_quota_bytes`: the staged bytes a destination accepts
    /// per source.
    pub peer_staging_quota_bytes: u64,
    /// `peer_staging_ttl_seconds`: when uncommitted staging is discarded.
    pub peer_staging_ttl_seconds: u64,
}

impl Default for PeeringConfig {
    fn default() -> Self {
        Self {
            quic_listen: SocketAddr::from(([0, 0, 0, 0], 7443)),
            congestion_control: CongestionControl::Cubic,
            peer_frame_bytes: 256 * KIB,
            peer_connect_timeout_ms: 3000,
            peer_connections_per_shard: 64,
            peer_max_inflight_bytes: 256 * MIB,
            peer_staging_quota_bytes: TIB,
            peer_staging_ttl_seconds: 86_400,
        }
    }
}

crate::durations! {
    PeeringConfig {
        /// `peer_connect_timeout_ms`.
        peer_connect_timeout => peer_connect_timeout_ms, Duration::from_millis;
        /// `peer_staging_ttl_seconds`.
        peer_staging_ttl => peer_staging_ttl_seconds, Duration::from_secs;
    }
}

impl PeeringConfig {
    pub(crate) fn check(&self, checker: &mut Checker) {
        // A destination stages each `DATA` frame as one log record (§7.8).
        let max_frame = u64::from(MAX_RECORD_PAYLOAD_LEN);
        checker.require(
            (1..=max_frame).contains(&self.peer_frame_bytes),
            "peering.peer_frame_bytes",
            || {
                format!(
                    "is {}; it must be from 1 to {max_frame}, the largest log record payload",
                    self.peer_frame_bytes
                )
            },
        );
        checker.nonzero(
            "peering.peer_connect_timeout_ms",
            self.peer_connect_timeout_ms,
        );
        checker.nonzero(
            "peering.peer_connections_per_shard",
            self.peer_connections_per_shard.into(),
        );
        checker.require(
            self.peer_max_inflight_bytes >= self.peer_frame_bytes,
            "peering.peer_max_inflight_bytes",
            || {
                format!(
                    "is {}; it must be at least peer_frame_bytes ({}), or no frame fits in \
                     flight",
                    self.peer_max_inflight_bytes, self.peer_frame_bytes
                )
            },
        );
        checker.require(
            self.peer_staging_quota_bytes >= self.peer_frame_bytes,
            "peering.peer_staging_quota_bytes",
            || {
                format!(
                    "is {}; it must be at least peer_frame_bytes ({})",
                    self.peer_staging_quota_bytes, self.peer_frame_bytes
                )
            },
        );
        checker.nonzero(
            "peering.peer_staging_ttl_seconds",
            self.peer_staging_ttl_seconds,
        );
    }
}
