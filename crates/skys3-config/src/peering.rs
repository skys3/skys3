//! `[peering]`: the native QUIC transport between SkyS3 clusters (§7.8).

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use skys3_types::limits::{MAX_RECORD_PAYLOAD_LEN, MIN_STAGING_CHARGE};
use skys3_types::{BucketId, BucketName, ClusterId};

use crate::error::{Checker, key_path};
use crate::storage::{KIB, MIB, TIB};
use crate::transport::TransportConfig;

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
    /// `quic_advertise`: the addresses, as `host:port`, that this node's
    /// peer descriptor names for its QUIC endpoint (§7.8). Empty: the
    /// node serves no descriptor, and sources reach it over S3 REST.
    pub quic_advertise: Vec<String>,
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
    /// `[peering.peers.<cluster-id>]`: the peer clusters this node trusts,
    /// by cluster ID (§12).
    pub peers: BTreeMap<ClusterId, PeerConfig>,
}

/// `[peering.peers.<cluster-id>]`: a peer cluster, its trust bundle, and
/// the bucket pairs it may write (§12).
///
/// Peer connections present the node's own certificate (`[transport]`).
/// The peer's chain must lead to the peer's own `ca_file`, so a CA that
/// one peer shares with another cannot vouch for the other's nodes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// `ca_file`: the CA certificates the peer's node certificates lead
    /// to, in PEM.
    pub ca_file: PathBuf,
    /// `buckets`: the pairs the peer may write as a source, from one of its
    /// buckets into one of this cluster's. Empty for a peer this node only
    /// sends to.
    #[serde(default)]
    pub buckets: Vec<BucketPair>,
    /// `s3_access_key_ids`: the access keys the peer's flushers sign with
    /// on this cluster's S3 endpoint, when they fall back to S3 REST
    /// (§7.8). A request signed with one of them is the peer's: it may
    /// write the buckets whose `peer_source` is the peer, with the peer's
    /// write identities.
    #[serde(default)]
    pub s3_access_key_ids: Vec<String>,
}

/// The most addresses `quic_advertise` lists.
pub const MAX_ADVERTISED_ADDRESSES: usize = 16;

/// The longest address `quic_advertise` lists, in bytes.
pub const MAX_ADVERTISED_ADDRESS_LEN: usize = 255;

/// A source bucket of a peer cluster, and the bucket of this cluster it
/// replicates into.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketPair {
    /// `source`: the peer's bucket, by the bucket ID its write identities
    /// carry (§7.2). A recreated source bucket has a new ID, so its
    /// predecessor's pair does not authorize it.
    pub source: BucketId,
    /// `destination`: this cluster's bucket, by name.
    pub destination: BucketName,
}

impl Default for PeeringConfig {
    fn default() -> Self {
        Self {
            quic_listen: SocketAddr::from(([0, 0, 0, 0], 7443)),
            quic_advertise: Vec::new(),
            congestion_control: CongestionControl::Cubic,
            peer_frame_bytes: 256 * KIB,
            peer_connect_timeout_ms: 3000,
            peer_connections_per_shard: 64,
            peer_max_inflight_bytes: 256 * MIB,
            peer_staging_quota_bytes: TIB,
            peer_staging_ttl_seconds: 86_400,
            peers: BTreeMap::new(),
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
    pub(crate) fn check(
        &self,
        cluster_id: Option<&ClusterId>,
        transport: &TransportConfig,
        checker: &mut Checker,
    ) {
        self.check_limits(checker);
        self.check_advertised(checker);
        let mut keys = BTreeSet::new();
        for (cluster, peer) in &self.peers {
            let table = key_path("peering.peers", cluster.as_str());
            checker.require(Some(cluster) != cluster_id, &table, || {
                format!("is this cluster's own ID ({cluster}); a peer is another cluster")
            });
            checker.require(transport.tls_files().is_some(), &table, || {
                "needs the [transport] tls_*_file keys: peer connections present the node's \
                 certificate"
                    .to_owned()
            });
            checker.require(
                !peer.ca_file.as_os_str().is_empty(),
                &format!("{table}.ca_file"),
                || "must not be empty".to_owned(),
            );
            let mut seen = BTreeSet::new();
            for pair in &peer.buckets {
                checker.require(seen.insert(pair), &format!("{table}.buckets"), || {
                    format!(
                        "lists the pair {} -> {} twice",
                        pair.source, pair.destination
                    )
                });
            }
            for key in &peer.s3_access_key_ids {
                checker.require(
                    !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphanumeric()),
                    &format!("{table}.s3_access_key_ids"),
                    || format!("{key:?} is not an access key ID: ASCII letters and digits"),
                );
                checker.require(
                    keys.insert(key),
                    &format!("{table}.s3_access_key_ids"),
                    || format!("{key} is listed twice, or for another peer too"),
                );
            }
        }
    }

    /// Each advertised address is `host:port`, with a host of letters,
    /// digits, `.`, `-`, or a bracketed IPv6 address, and a nonzero port.
    fn check_advertised(&self, checker: &mut Checker) {
        let key = "peering.quic_advertise";
        checker.require(
            self.quic_advertise.len() <= MAX_ADVERTISED_ADDRESSES,
            key,
            || format!("lists more than {MAX_ADVERTISED_ADDRESSES} addresses"),
        );
        for address in &self.quic_advertise {
            checker.require(advertisable(address), key, || {
                format!(
                    "{address:?} is not host:port, with a port from 1 to 65535, in at most \
                     {MAX_ADVERTISED_ADDRESS_LEN} bytes"
                )
            });
        }
    }

    fn check_limits(&self, checker: &mut Checker) {
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
        // One staging and one frame, each charged at least the minimum.
        let least = MIN_STAGING_CHARGE + self.peer_frame_bytes.max(MIN_STAGING_CHARGE);
        checker.require(
            self.peer_staging_quota_bytes >= least,
            "peering.peer_staging_quota_bytes",
            || {
                format!(
                    "is {}; it must be at least {least}, what one staging and one frame of \
                     peer_frame_bytes ({}) are charged",
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

/// Whether `address` is `host:port` as `quic_advertise` takes it.
#[must_use]
pub fn advertisable(address: &str) -> bool {
    let Some((host, port)) = address.rsplit_once(':') else {
        return false;
    };
    let host_ok = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(v6) => v6.parse::<std::net::Ipv6Addr>().is_ok(),
        None => {
            !host.is_empty()
                && host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        }
    };
    address.len() <= MAX_ADVERTISED_ADDRESS_LEN
        && host_ok
        && port.parse::<u16>().is_ok_and(|port| port != 0)
}
