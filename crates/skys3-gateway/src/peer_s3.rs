//! What the S3 endpoint does for peer clusters (design §7.8): it serves
//! the peer descriptor of each bucket that receives native replication,
//! and takes a peer's flushes over S3 REST when its QUIC path is down.
//!
//! - **Descriptors.** A GET or HEAD of [`DESCRIPTOR_KEY`] in a bucket
//!   whose `peer_source` is set answers the descriptor the node's
//!   [`PeerDescriptors`] signs, after the usual authentication and
//!   authorization of a GetObject of that key. Without a descriptor, the
//!   key is read as any other, and a source finds no peer.
//! - **The key is reserved.** No write reaches it in any bucket, from a
//!   client or a peer, over S3 or in a `COMMIT`: PUT, copies to it,
//!   multipart uploads, tagging, and deletes answer `400 InvalidArgument`
//!   ([`writable_key`]), so no version of it is ever replicated, and a
//!   receiving bucket's descriptor never hides one.
//! - **Peer callers.** A request signed with one of the access keys a
//!   `[peering.peers.<cluster-id>]` table lists (`s3_access_key_ids`) is
//!   that peer's flusher ([`PeerCaller`]). It may write the buckets whose
//!   `peer_source` is the peer, which this cluster's own clients may not
//!   unless `peer_local_writes` is set. Its writes may carry the source's
//!   write identity (`x-amz-meta-skys3-wid`) when the identity names the
//!   peer and a bucket pair the peer is authorized for (§12); the version
//!   then carries it as a `COMMIT`'s does, so a later `COMMIT` of the
//!   other transport sees the same current identity. Its reads see the
//!   identity, as the flusher's 412 recovery needs (§7.2).
//! - **Apply-by times.** A peer's write that publishes or deletes a
//!   version (`PUT`, copy, multipart completion, delete, multi-object
//!   delete, and tagging) may carry an apply-by time in
//!   [`WriteIdentity::APPLY_BY_HEADER`], milliseconds since the Unix
//!   epoch, as its `COMMIT`s do: the key's shard refuses to sequence it
//!   later ([`Precondition::ApplyBy`](crate::Precondition::ApplyBy)), with
//!   `503 ServiceUnavailable`, so a write held on the way cannot apply
//!   after the peer moved back to QUIC. Where native `COMMIT`s can land,
//!   to a bucket that receives from the peer on a node that serves its
//!   descriptor, the header is required ([`ApplyBy::of_request`]). The
//!   gateway reads it only from a peer's keys.
//! - **Refusals** ([`PeerRefusals`], §7.6). The writes a peer's flusher
//!   gets refused for its configuration, not for the key, are counted in
//!   `skys3_peer_s3_refusals_total{peer, reason}`: a bucket whose
//!   `peer_source` is not the peer (`peer_source`), a write identity for a
//!   bucket pair the peer lacks (`bucket_pair`), and a request whose
//!   `x-skys3-*` headers are not signed (`unsigned_header`, whose `peer`
//!   is `unknown`, since the request is refused before it is
//!   authenticated). The source keeps retrying them, so a count that keeps
//!   rising is a configuration to fix on one side or the other.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use http::{Extensions, HeaderMap};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use skys3_config::PeeringConfig;
use skys3_obs::MetricsRegistry;
use skys3_types::{BucketId, BucketName, ClusterId, WriteIdentity};

use s3s::{S3Error, S3Result, s3_error};
pub use skys3_peer::DESCRIPTOR_KEY;

use crate::sigv4::Authenticated;

/// Refuses a write of `key` if it is [`DESCRIPTOR_KEY`], which only the
/// peer descriptor may answer (§7.8).
///
/// # Errors
///
/// `400 InvalidArgument` for the reserved key.
pub(crate) fn writable_key(key: &str) -> S3Result<()> {
    if key == DESCRIPTOR_KEY {
        return Err(reserved_key());
    }
    Ok(())
}

/// The refusal of a write of [`DESCRIPTOR_KEY`].
pub(crate) fn reserved_key() -> S3Error {
    s3_error!(
        InvalidArgument,
        "The key {DESCRIPTOR_KEY} is reserved for SkyS3's peer descriptor"
    )
}

/// Serves the signed peer descriptors of a node's receiving buckets.
pub trait PeerDescriptors: Send + Sync + std::fmt::Debug + 'static {
    /// The signed descriptor of `bucket`, which receives native
    /// replication from `source`, or `None` if the node serves none.
    fn descriptor(&self, bucket: &BucketName, source: &ClusterId) -> Option<Bytes>;
}

/// Why the gateway refused a peer cluster's S3 request (§7.8), as the
/// `reason` label of `skys3_peer_s3_refusals_total` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerRefusal {
    /// An object write by a peer's flusher to a bucket whose `peer_source`
    /// is not that peer: `403 AccessDenied`.
    PeerSource,
    /// A peer's write whose write identity names another cluster, or a
    /// bucket pair the peer's `[peering.peers]` table does not list: `400
    /// InvalidArgument`.
    BucketPair,
    /// A request with `x-skys3-*` headers that are not signed: `403
    /// AccessDenied`, before the request is authenticated.
    UnsignedHeader,
}

impl PeerRefusal {
    /// Every reason, in label order.
    pub const ALL: [PeerRefusal; 3] = [
        PeerRefusal::PeerSource,
        PeerRefusal::BucketPair,
        PeerRefusal::UnsignedHeader,
    ];

    /// The label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            PeerRefusal::PeerSource => "peer_source",
            PeerRefusal::BucketPair => "bucket_pair",
            PeerRefusal::UnsignedHeader => "unsigned_header",
        }
    }
}

/// The `peer` label of a refusal whose sender is not authenticated.
pub const UNKNOWN_PEER: &str = "unknown";

/// `skys3_peer_s3_refusals_total{peer, reason}`: the requests of peer
/// clusters this node's S3 endpoint refused for their configuration
/// ([`PeerRefusal`]). `peer` is the peer cluster an access key in
/// `s3_access_key_ids` authenticated, or [`UNKNOWN_PEER`]; both labels take
/// values from bounded sets. Clones share the counters; the default
/// belongs to no registry.
#[derive(Debug, Clone, Default)]
pub struct PeerRefusals {
    counters: Family<Vec<(String, String)>, Counter>,
}

impl PeerRefusals {
    /// Registers the counters in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let refusals = Self::default();
        registry.register(
            "peer_s3_refusals",
            "Requests of peer SkyS3 clusters' flushers this node's S3 endpoint refused for \
             their configuration, by peer (the authenticated peer cluster, or unknown) and \
             reason: peer_source (the bucket does not receive from the peer), bucket_pair (a \
             write identity for a bucket pair the peer lacks), or unsigned_header (unsigned \
             x-skys3-* headers).",
            refusals.counters.clone(),
        );
        refusals
    }

    /// Counts a refusal of a request from `peer` (`None`: not
    /// authenticated) for `reason`.
    pub fn count(&self, peer: Option<&ClusterId>, reason: PeerRefusal) {
        self.counters.get_or_create(&labels(peer, reason)).inc();
    }

    /// How many refusals of `peer`'s requests for `reason` were counted.
    #[must_use]
    pub fn get(&self, peer: Option<&ClusterId>, reason: PeerRefusal) -> u64 {
        self.counters
            .get(&labels(peer, reason))
            .map_or(0, |counter| counter.get())
    }
}

fn labels(peer: Option<&ClusterId>, reason: PeerRefusal) -> Vec<(String, String)> {
    let peer = peer.map_or(UNKNOWN_PEER, ClusterId::as_str);
    vec![
        ("peer".to_owned(), peer.to_owned()),
        ("reason".to_owned(), reason.as_str().to_owned()),
    ]
}

/// A request from a peer cluster's flusher over S3 REST: signed with one
/// of the access keys the peer's table lists. The gateway adds it to the
/// request's extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerCaller(pub(crate) ClusterId);

/// Which access keys belong to peer clusters, and the bucket pairs each
/// peer may write (§12).
#[derive(Debug, Clone, Default)]
pub struct PeerAccess {
    keys: Arc<BTreeMap<String, ClusterId>>,
    pairs: Arc<BTreeSet<(ClusterId, BucketId, BucketName)>>,
    refusals: PeerRefusals,
}

impl PeerAccess {
    /// The access keys and bucket pairs of every `[peering.peers]` table.
    #[must_use]
    pub fn from_config(config: &PeeringConfig) -> Self {
        let mut keys = BTreeMap::new();
        let mut pairs = BTreeSet::new();
        for (cluster, peer) in &config.peers {
            for key in &peer.s3_access_key_ids {
                keys.insert(key.clone(), cluster.clone());
            }
            for pair in &peer.buckets {
                pairs.insert((
                    cluster.clone(),
                    pair.source.clone(),
                    pair.destination.clone(),
                ));
            }
        }
        Self {
            keys: Arc::new(keys),
            pairs: Arc::new(pairs),
            refusals: PeerRefusals::default(),
        }
    }

    /// Counts the refusals of peers' requests in `refusals`.
    #[must_use]
    pub fn with_refusals(mut self, refusals: PeerRefusals) -> Self {
        self.refusals = refusals;
        self
    }

    /// Where the refusals of peers' requests are counted.
    #[must_use]
    pub fn refusals(&self) -> &PeerRefusals {
        &self.refusals
    }

    /// The peer whose access key signed the request with `extensions`, if
    /// any; it is also recorded there as a [`PeerCaller`].
    pub(crate) fn caller(&self, extensions: &mut Extensions) -> Option<ClusterId> {
        let peer = extensions
            .get::<Authenticated>()
            .and_then(|auth| self.keys.get(&auth.access_key_id))
            .cloned()?;
        extensions.insert(PeerCaller(peer.clone()));
        Some(peer)
    }

    /// Whether the peer `caller` may store the write identity `value` with
    /// an object of `bucket`: it names the peer and an authorized pair.
    pub(crate) fn may_carry(&self, caller: &ClusterId, bucket: &BucketName, value: &str) -> bool {
        value.parse::<WriteIdentity>().is_ok_and(|identity| {
            &identity.cluster == caller
                && self
                    .pairs
                    .contains(&(caller.clone(), identity.bucket, bucket.clone()))
        })
    }
}

/// Who may set the write identity on a write.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Carrier<'a> {
    /// Nobody: a client's write.
    Client,
    /// A peer's flusher, writing `bucket`.
    Peer {
        access: &'a PeerAccess,
        caller: &'a ClusterId,
        bucket: &'a BucketName,
    },
}

impl<'a> Carrier<'a> {
    /// The carrier of a write to `bucket` by the request whose
    /// `extensions` the gateway marked.
    pub(crate) fn of(
        access: &'a PeerAccess,
        extensions: &'a Extensions,
        bucket: &'a BucketName,
    ) -> Self {
        match extensions.get::<PeerCaller>() {
            Some(PeerCaller(caller)) => Carrier::Peer {
                access,
                caller,
                bucket,
            },
            None => Carrier::Client,
        }
    }

    /// Whether the write may store the write identity `value`. A peer's
    /// write that may not is counted as refused for its bucket pair.
    pub(crate) fn accepts(&self, value: &str) -> bool {
        match self {
            Carrier::Client => false,
            Carrier::Peer {
                access,
                caller,
                bucket,
            } => {
                let accepted = access.may_carry(caller, bucket, value);
                if !accepted {
                    access.refusals.count(Some(caller), PeerRefusal::BucketPair);
                }
                accepted
            }
        }
    }
}

/// The apply-by time of a peer's write, in milliseconds since the Unix
/// epoch, which the gateway records in the request's extensions (§7.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ApplyBy(pub(crate) u64);

impl ApplyBy {
    /// Reads the apply-by time of a write to `bucket` from `headers` into
    /// `extensions`, if a peer's flusher sent it ([`PeerCaller`]); a
    /// client's is ignored. `required`: the bucket receives from that peer
    /// on a node that serves its descriptor, where native `COMMIT`s land,
    /// so its S3 writes must be bounded too.
    ///
    /// # Errors
    ///
    /// `400 InvalidArgument` for a malformed header, or none where it is
    /// required.
    pub(crate) fn of_request(
        headers: &HeaderMap,
        extensions: &mut Extensions,
        required: bool,
    ) -> S3Result<()> {
        if extensions.get::<PeerCaller>().is_none() {
            return Ok(());
        }
        let name = WriteIdentity::APPLY_BY_HEADER;
        let Some(value) = headers.get(name) else {
            if required {
                return Err(s3_error!(
                    InvalidArgument,
                    "A peer cluster's write to a bucket that receives native replication from \
                     it must carry {name} (design 7.8)"
                ));
            }
            return Ok(());
        };
        let apply_by = value
            .to_str()
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                s3_error!(
                    InvalidArgument,
                    "{name} must be milliseconds since the Unix epoch"
                )
            })?;
        extensions.insert(ApplyBy(apply_by));
        Ok(())
    }

    /// The apply-by time the request with `extensions` carries, if any.
    pub(crate) fn of(extensions: &Extensions) -> Option<u64> {
        extensions
            .get::<ApplyBy>()
            .map(|ApplyBy(apply_by)| *apply_by)
    }
}

/// Whether a read by the request with `extensions` sees the write
/// identity a version carries: only a peer's flusher does.
pub(crate) fn reveals_identity(extensions: &Extensions) -> bool {
    extensions.get::<PeerCaller>().is_some()
}

#[cfg(test)]
mod tests {
    use skys3_config::{BucketPair, PeerConfig};

    use super::*;
    use crate::authz::{Permissions, Principal};
    use crate::sigv4::AuthMethod;

    fn access() -> PeerAccess {
        let mut config = PeeringConfig::default();
        config.peers.insert(
            ClusterId::new("prod-us").unwrap(),
            PeerConfig {
                ca_file: "/us.crt".into(),
                buckets: vec![BucketPair {
                    source: BucketId::new("b-src").unwrap(),
                    destination: BucketName::new("archive").unwrap(),
                }],
                s3_access_key_ids: vec!["AKIAUS".to_owned()],
            },
        );
        PeerAccess::from_config(&config)
    }

    fn signed_with(key: &str) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(Authenticated {
            principal: Principal::new("flusher", Permissions::allow_all()),
            access_key_id: key.to_owned(),
            method: AuthMethod::Header,
        });
        extensions
    }

    #[test]
    fn peer_keys_mark_their_requests() {
        let access = access();
        let mut extensions = signed_with("AKIAUS");
        let us = ClusterId::new("prod-us").unwrap();
        assert_eq!(access.caller(&mut extensions), Some(us.clone()));
        assert!(reveals_identity(&extensions));
        let mut client = signed_with("AKIACLIENT");
        assert_eq!(access.caller(&mut client), None);
        assert!(!reveals_identity(&client));
        assert_eq!(access.caller(&mut Extensions::new()), None);

        let archive = BucketName::new("archive").unwrap();
        let carrier = Carrier::of(&access, &extensions, &archive);
        assert!(carrier.accepts("prod-us/b-src/3/2.17"));
        // Another cluster's identity, an unpaired source bucket, another
        // destination, or no identity at all.
        assert!(!carrier.accepts("prod-ap/b-src/3/2.17"));
        assert!(!carrier.accepts("prod-us/b-other/3/2.17"));
        assert!(!carrier.accepts("not an identity"));
        let other = BucketName::new("other").unwrap();
        assert!(!Carrier::of(&access, &extensions, &other).accepts("prod-us/b-src/3/2.17"));
        assert!(!Carrier::of(&access, &client, &archive).accepts("prod-us/b-src/3/2.17"));
    }

    #[test]
    fn refusals_are_exported_by_peer_and_reason() {
        let registry = MetricsRegistry::new();
        let access = access().with_refusals(PeerRefusals::register(&registry));
        let us = ClusterId::new("prod-us").unwrap();
        let mut extensions = signed_with("AKIAUS");
        access.caller(&mut extensions);
        let archive = BucketName::new("archive").unwrap();
        // A refused identity counts against the peer's bucket pair.
        let carrier = Carrier::of(&access, &extensions, &archive);
        assert!(!carrier.accepts("prod-us/b-other/1/1.1"));
        access.refusals().count(None, PeerRefusal::UnsignedHeader);
        assert_eq!(access.refusals().get(Some(&us), PeerRefusal::BucketPair), 1);
        assert_eq!(access.refusals().get(Some(&us), PeerRefusal::PeerSource), 0);
        let text = registry.encode().unwrap();
        for line in [
            "skys3_peer_s3_refusals_total{peer=\"prod-us\",reason=\"bucket_pair\"} 1",
            "skys3_peer_s3_refusals_total{peer=\"unknown\",reason=\"unsigned_header\"} 1",
        ] {
            assert!(text.contains(line), "{line} is not in {text}");
        }
        assert_eq!(
            PeerRefusal::ALL.map(PeerRefusal::as_str),
            ["peer_source", "bucket_pair", "unsigned_header"]
        );
    }
}
