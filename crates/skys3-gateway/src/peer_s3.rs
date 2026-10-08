//! What the S3 endpoint does for peer clusters (design §7.8): it serves
//! the peer descriptor of each bucket that receives native replication,
//! and takes a peer's flushes over S3 REST when its QUIC path is down.
//!
//! - **Descriptors.** A GET or HEAD of [`DESCRIPTOR_KEY`] in a bucket
//!   whose `peer_source` is set answers the descriptor the node's
//!   [`PeerDescriptors`] signs, after the usual authentication and
//!   authorization of a GetObject of that key. Without a descriptor, the
//!   key is read as any other, and a source finds no peer.
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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use http::Extensions;
use skys3_config::PeeringConfig;
use skys3_types::{BucketId, BucketName, ClusterId, WriteIdentity};

pub use skys3_peer::DESCRIPTOR_KEY;

use crate::sigv4::Authenticated;

/// Serves the signed peer descriptors of a node's receiving buckets.
pub trait PeerDescriptors: Send + Sync + std::fmt::Debug + 'static {
    /// The signed descriptor of `bucket`, which receives native
    /// replication from `source`, or `None` if the node serves none.
    fn descriptor(&self, bucket: &BucketName, source: &ClusterId) -> Option<Bytes>;
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
        }
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

    /// Whether the write may store the write identity `value`.
    pub(crate) fn accepts(&self, value: &str) -> bool {
        match self {
            Carrier::Client => false,
            Carrier::Peer {
                access,
                caller,
                bucket,
            } => access.may_carry(caller, bucket, value),
        }
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
}
