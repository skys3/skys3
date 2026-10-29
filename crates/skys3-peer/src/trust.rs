//! Which peer clusters a node trusts, and what each may write (design
//! §12).
//!
//! Each peer cluster has its own CA bundle, and its nodes' certificates
//! must lead to that bundle and name the peer's cluster as their SPIFFE
//! trust domain. A CA that two peers share therefore cannot vouch for one
//! peer's nodes as the other's.
//!
//! A peer writes only the bucket pairs it is authorized for: a source
//! bucket of its own, named by the bucket ID its write identities carry,
//! and a destination bucket of this cluster, by name.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};

use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, TrustAnchor};
use skys3_config::{BucketPair, PeeringConfig};
use skys3_types::{BucketId, BucketName, ClusterId, WriteIdentity};

/// Why the peer trust bundles could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TrustError {
    /// A CA file could not be read.
    #[error("cannot read {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// Why it could not be read.
        source: io::Error,
    },
    /// A CA bundle is not valid PEM.
    #[error("invalid PEM in the CA bundle of peer {cluster}: {source}")]
    Pem {
        /// The peer cluster.
        cluster: ClusterId,
        /// What is wrong with it.
        source: pem::Error,
    },
    /// A CA bundle holds no certificate.
    #[error("the CA bundle of peer {0} has no certificate")]
    NoCa(ClusterId),
    /// A CA certificate cannot be parsed.
    #[error("invalid certificate in the CA bundle of peer {cluster}: {source}")]
    Certificate {
        /// The peer cluster.
        cluster: ClusterId,
        /// What is wrong with it.
        source: webpki::Error,
    },
}

/// Why a peer may not write what a message names.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Unauthorized {
    /// The write identity names another cluster than the peer's own, so
    /// the peer would write in another cluster's name.
    #[error("peer {peer} sent write identity {identity}, which is not one of its own")]
    ForeignIdentity {
        /// The authenticated peer cluster.
        peer: ClusterId,
        /// The write identity it sent.
        identity: WriteIdentity,
    },
    /// The peer is not authorized for the bucket pair.
    #[error("peer {peer} may not write its bucket {bucket} into bucket {destination}")]
    BucketPair {
        /// The authenticated peer cluster.
        peer: ClusterId,
        /// The source bucket, from the write identity.
        bucket: BucketId,
        /// The destination bucket the message names.
        destination: BucketName,
    },
    /// The peer has no pair with this source bucket, for a message that
    /// names no destination bucket (`ABORT`).
    #[error("peer {peer} has no authorized pair from its bucket {bucket}")]
    SourceBucket {
        /// The authenticated peer cluster.
        peer: ClusterId,
        /// The source bucket, from the write identity.
        bucket: BucketId,
    },
}

/// The peer clusters a node trusts: each one's CA certificates and the
/// bucket pairs it may write.
#[derive(Debug, Default)]
pub struct PeerTrust {
    peers: BTreeMap<ClusterId, TrustedPeer>,
}

#[derive(Debug)]
struct TrustedPeer {
    roots: Vec<TrustAnchor<'static>>,
    pairs: BTreeSet<(BucketId, BucketName)>,
}

impl PeerTrust {
    /// Trust in no peer cluster.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the CA bundle of every `[peering.peers]` entry. The files are
    /// read with blocking I/O, once at startup.
    ///
    /// # Errors
    ///
    /// [`TrustError::Read`] for a file that cannot be read, and the errors
    /// of [`PeerTrust::add_pem`].
    pub fn load(config: &PeeringConfig) -> Result<Self, TrustError> {
        let mut trust = Self::new();
        for (cluster, peer) in &config.peers {
            let pem = read(&peer.ca_file)?;
            trust.add_pem(cluster.clone(), &pem, peer.buckets.iter().cloned())?;
        }
        Ok(trust)
    }

    /// Trusts `cluster` with the CA certificates of the PEM bundle
    /// `ca_pem`, for the bucket `pairs`.
    ///
    /// # Errors
    ///
    /// [`TrustError::Pem`] for malformed PEM, and the errors of
    /// [`PeerTrust::add`].
    pub fn add_pem(
        &mut self,
        cluster: ClusterId,
        ca_pem: &[u8],
        pairs: impl IntoIterator<Item = BucketPair>,
    ) -> Result<(), TrustError> {
        let ca = CertificateDer::pem_slice_iter(ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| TrustError::Pem {
                cluster: cluster.clone(),
                source,
            })?;
        self.add(cluster, &ca, pairs)
    }

    /// Trusts `cluster` with the CA certificates `ca`, in DER, for the
    /// bucket `pairs`. Trusting a cluster again replaces its entry.
    ///
    /// # Errors
    ///
    /// [`TrustError::NoCa`] for an empty bundle, and
    /// [`TrustError::Certificate`] for a certificate that cannot be parsed.
    pub fn add(
        &mut self,
        cluster: ClusterId,
        ca: &[CertificateDer<'_>],
        pairs: impl IntoIterator<Item = BucketPair>,
    ) -> Result<(), TrustError> {
        let roots = ca
            .iter()
            .map(|cert| webpki::anchor_from_trusted_cert(cert).map(|anchor| anchor.to_owned()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| TrustError::Certificate {
                cluster: cluster.clone(),
                source,
            })?;
        if roots.is_empty() {
            return Err(TrustError::NoCa(cluster));
        }
        let pairs = pairs
            .into_iter()
            .map(|pair| (pair.source, pair.destination))
            .collect();
        self.peers.insert(cluster, TrustedPeer { roots, pairs });
        Ok(())
    }

    /// Whether `cluster` is a trusted peer.
    #[must_use]
    pub fn trusts(&self, cluster: &ClusterId) -> bool {
        self.peers.contains_key(cluster)
    }

    /// The CA certificates of a trusted peer.
    pub(crate) fn roots(&self, cluster: &ClusterId) -> Option<&[TrustAnchor<'static>]> {
        self.peers.get(cluster).map(|peer| peer.roots.as_slice())
    }

    /// Checks that `peer` may write `identity` into `destination`: the
    /// identity names `peer`'s own cluster, and its bucket and
    /// `destination` are an authorized pair. Without a destination, as for
    /// an `ABORT`, any authorized pair from the identity's bucket will do.
    ///
    /// # Errors
    ///
    /// [`Unauthorized`] saying which rule fails.
    pub fn authorize(
        &self,
        peer: &ClusterId,
        identity: &WriteIdentity,
        destination: Option<&BucketName>,
    ) -> Result<(), Unauthorized> {
        if &identity.cluster != peer {
            return Err(Unauthorized::ForeignIdentity {
                peer: peer.clone(),
                identity: identity.clone(),
            });
        }
        let pairs = self.peers.get(peer).map(|trusted| &trusted.pairs);
        let source = &identity.bucket;
        match destination {
            Some(destination) => {
                let pair = (source.clone(), destination.clone());
                if pairs.is_some_and(|pairs| pairs.contains(&pair)) {
                    Ok(())
                } else {
                    Err(Unauthorized::BucketPair {
                        peer: peer.clone(),
                        bucket: pair.0,
                        destination: pair.1,
                    })
                }
            }
            None => {
                if pairs.is_some_and(|pairs| pairs.iter().any(|(s, _)| s == source)) {
                    Ok(())
                } else {
                    Err(Unauthorized::SourceBucket {
                        peer: peer.clone(),
                        bucket: source.clone(),
                    })
                }
            }
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>, TrustError> {
    std::fs::read(path).map_err(|source| TrustError::Read {
        path: path.to_owned(),
        source,
    })
}
