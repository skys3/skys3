//! The peer descriptor (design §7.8): how a destination cluster tells a
//! source, over its S3 endpoint, that it is a SkyS3 cluster and where its
//! QUIC endpoint is.
//!
//! - **Where.** A destination bucket that receives native replication
//!   (`peer_source`) answers a GET of the reserved key [`DESCRIPTOR_KEY`]
//!   with a descriptor, so a source asks through the same store, with the
//!   same credentials, that its S3 REST flushes use. Other buckets, and
//!   other stores, answer `404`, and the source flushes over S3 REST.
//! - **What.** The destination cluster, the bucket, the source cluster the
//!   bucket receives from, the QUIC addresses (`quic_advertise`), the
//!   protocol versions, and when it was issued and expires
//!   ([`DESCRIPTOR_LIFETIME`] later).
//! - **Signed** by the node that serves it, with the private key of its
//!   `[transport]` certificate, the identity it presents on peer
//!   connections. The descriptor carries the certificate chain, so the
//!   source needs no key ID: it verifies the chain against the configured
//!   CA bundle of the cluster the descriptor names, as the QUIC handshake
//!   does (§12), and the signature with the leaf's key. Certificates rotate
//!   as node certificates do, and a descriptor outlives a rotation by at
//!   most its lifetime.
//! - **Replay.** A replayed descriptor can only point a source at the
//!   addresses it named while it was valid, and only for its own bucket
//!   and source: the QUIC handshake authenticates the destination again,
//!   so a stale or replayed descriptor never lets anyone else receive the
//!   source's writes. Expiry bounds how long old addresses are tried. A
//!   descriptor removed or tampered with in transit only makes the source
//!   use S3 REST, as a blocked UDP path does.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use rustls::SignatureScheme;
use rustls::crypto::{WebPkiSupportedAlgorithms, aws_lc_rs};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::sign::CertifiedKey;
use skys3_config::{MAX_ADVERTISED_ADDRESSES, advertisable};
use skys3_types::{BucketName, ClusterId};
use webpki::{EndEntityCert, KeyUsage};

use crate::negotiation::{SUPPORTED_VERSIONS, VersionRange};
use crate::tls::PeerVerifier;
use crate::trust::PeerTrust;

/// The key, at the root of a destination bucket, whose GET answers the
/// bucket's peer descriptor.
pub const DESCRIPTOR_KEY: &str = ".skys3/peer-descriptor";

/// How long a descriptor a node issues is valid.
pub const DESCRIPTOR_LIFETIME: Duration = Duration::from_secs(3600);

/// The longest validity a source accepts: a descriptor that claims more
/// is refused, so a leaked one cannot be replayed for long.
pub const MAX_DESCRIPTOR_LIFETIME: Duration = Duration::from_secs(86_400);

/// How far a descriptor's issue time may lie in the source's future: the
/// clock difference between two clusters a source tolerates.
pub const DESCRIPTOR_CLOCK_SKEW: Duration = Duration::from_secs(300);

/// The largest encoded descriptor a source reads.
pub const MAX_DESCRIPTOR_LEN: usize = 64 * 1024;

/// The most certificates a descriptor's chain holds, its leaf included.
pub const MAX_CHAIN_LEN: usize = 4;

/// The encoding of the descriptor body this implementation writes.
const FORMAT: u32 = 1;

/// What the signature covers before the body, so that a descriptor
/// signature can never pass for any other signature of the same key.
const CONTEXT: &[u8] = b"skys3 peer descriptor\0";

/// The signature schemes a descriptor may be signed with: those of TLS
/// 1.3, as the key signs peer handshakes with.
const SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::ECDSA_NISTP256_SHA256,
    SignatureScheme::ECDSA_NISTP384_SHA384,
    SignatureScheme::ED25519,
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA512,
];

/// A destination bucket's peer descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerDescriptor {
    /// The destination cluster.
    pub cluster: ClusterId,
    /// The destination bucket.
    pub bucket: BucketName,
    /// The cluster the bucket receives native replication from.
    pub source: ClusterId,
    /// The QUIC endpoint's addresses, as `host:port`: one to
    /// [`MAX_ADVERTISED_ADDRESSES`].
    pub addresses: Vec<String>,
    /// The protocol versions the destination speaks.
    pub versions: VersionRange,
    /// When it was issued, as time since the Unix epoch.
    pub issued: Duration,
    /// When it expires, as time since the Unix epoch.
    pub expires: Duration,
}

/// Why a descriptor was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DescriptorError {
    /// Longer than [`MAX_DESCRIPTOR_LEN`].
    #[error("the descriptor is {0} bytes, more than {MAX_DESCRIPTOR_LEN}")]
    TooLong(usize),
    /// Not a descriptor.
    #[error("the descriptor is malformed: {0}")]
    Malformed(String),
    /// A body encoding this implementation does not read.
    #[error("the descriptor's format {0} is not known")]
    Format(u32),
    /// Its chain is missing or too long.
    #[error("the descriptor carries {0} certificates; it needs 1 to {MAX_CHAIN_LEN}")]
    Chain(usize),
    /// The chain does not lead to the CA bundle of the cluster named.
    #[error("the descriptor's certificate is not trusted: {0}")]
    Untrusted(String),
    /// The signature is not the leaf key's over the body.
    #[error("the descriptor's signature does not verify")]
    Signature,
    /// Issued after the source's clock plus [`DESCRIPTOR_CLOCK_SKEW`].
    #[error("the descriptor is issued in the future")]
    NotYetValid,
    /// Expired.
    #[error("the descriptor expired")]
    Expired,
    /// Valid for longer than [`MAX_DESCRIPTOR_LIFETIME`].
    #[error("the descriptor claims a validity longer than {MAX_DESCRIPTOR_LIFETIME:?}")]
    Lifetime,
    /// For another bucket than the one the source flushes to.
    #[error("the descriptor is for bucket {found}, not {expected}")]
    Bucket {
        /// The bucket the source flushes to.
        expected: BucketName,
        /// The bucket the descriptor names.
        found: BucketName,
    },
    /// The bucket receives from another cluster than the source.
    #[error("the bucket receives native replication from {found}, not from {expected}")]
    Source {
        /// The source's cluster.
        expected: ClusterId,
        /// The cluster the descriptor names.
        found: ClusterId,
    },
    /// No protocol version both ends speak.
    #[error("the destination speaks no protocol version this node speaks")]
    Versions,
    /// The node cannot sign: its key offers no TLS 1.3 scheme.
    #[error("the node's key cannot sign a descriptor")]
    Unsignable,
}

/// The signed envelope: the body's bytes, exactly as signed.
#[derive(Clone, PartialEq, prost::Message)]
struct WireSigned {
    #[prost(bytes = "vec", tag = "1")]
    body: Vec<u8>,
    #[prost(uint32, tag = "2")]
    scheme: u32,
    #[prost(bytes = "vec", tag = "3")]
    signature: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    chain: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireBody {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    cluster: String,
    #[prost(string, tag = "3")]
    bucket: String,
    #[prost(string, tag = "4")]
    source: String,
    #[prost(string, repeated, tag = "5")]
    addresses: Vec<String>,
    #[prost(uint32, tag = "6")]
    min_version: u32,
    #[prost(uint32, tag = "7")]
    max_version: u32,
    #[prost(uint64, tag = "8")]
    issued_unix_ms: u64,
    #[prost(uint64, tag = "9")]
    expires_unix_ms: u64,
}

impl PeerDescriptor {
    /// The descriptor of `bucket`, which receives from `source`, at the
    /// QUIC `addresses` of `cluster`, issued at `now` and valid for
    /// [`DESCRIPTOR_LIFETIME`].
    #[must_use]
    pub fn new(
        cluster: ClusterId,
        bucket: BucketName,
        source: ClusterId,
        addresses: Vec<String>,
        now: Duration,
    ) -> Self {
        Self {
            cluster,
            bucket,
            source,
            addresses,
            versions: SUPPORTED_VERSIONS,
            issued: now,
            expires: now + DESCRIPTOR_LIFETIME,
        }
    }

    /// Reads a descriptor's fields without checking its chain, signature,
    /// or times: only what [`DescriptorVerifier::verify`] trusts after its
    /// checks.
    ///
    /// # Errors
    ///
    /// [`DescriptorError`] for anything that is not a well-formed
    /// descriptor.
    pub fn decode_unverified(bytes: &[u8]) -> Result<Self, DescriptorError> {
        Ok(Signed::decode(bytes)?.descriptor)
    }

    fn body(&self) -> Vec<u8> {
        let millis = |at: Duration| u64::try_from(at.as_millis()).unwrap_or(u64::MAX);
        WireBody {
            format: FORMAT,
            cluster: self.cluster.to_string(),
            bucket: self.bucket.to_string(),
            source: self.source.to_string(),
            addresses: self.addresses.clone(),
            min_version: self.versions.min().into(),
            max_version: self.versions.max().into(),
            issued_unix_ms: millis(self.issued),
            expires_unix_ms: millis(self.expires),
        }
        .encode_to_vec()
    }

    fn from_body(bytes: &[u8]) -> Result<Self, DescriptorError> {
        let malformed = |what: &dyn std::fmt::Display| DescriptorError::Malformed(what.to_string());
        let wire = WireBody::decode(bytes).map_err(|e| malformed(&e))?;
        if wire.format != FORMAT {
            return Err(DescriptorError::Format(wire.format));
        }
        let cluster = ClusterId::new(&wire.cluster).map_err(|e| malformed(&e))?;
        let source = ClusterId::new(&wire.source).map_err(|e| malformed(&e))?;
        let bucket = BucketName::new(&wire.bucket).map_err(|e| malformed(&e))?;
        if wire.addresses.is_empty() || wire.addresses.len() > MAX_ADVERTISED_ADDRESSES {
            return Err(malformed(&format_args!(
                "it lists {} addresses",
                wire.addresses.len()
            )));
        }
        if let Some(bad) = wire.addresses.iter().find(|a| !advertisable(a)) {
            return Err(malformed(&format_args!("{bad:?} is not host:port")));
        }
        let version = |v: u32| u16::try_from(v).unwrap_or(0);
        let versions = VersionRange::new(version(wire.min_version), version(wire.max_version))
            .ok_or_else(|| malformed(&"its version range is empty"))?;
        let (issued, expires) = (
            Duration::from_millis(wire.issued_unix_ms),
            Duration::from_millis(wire.expires_unix_ms),
        );
        if expires <= issued {
            return Err(malformed(&"it expires before it is issued"));
        }
        Ok(Self {
            cluster,
            bucket,
            source,
            addresses: wire.addresses,
            versions,
            issued,
            expires,
        })
    }
}

/// A decoded envelope and the descriptor it holds.
struct Signed {
    descriptor: PeerDescriptor,
    message: Vec<u8>,
    scheme: SignatureScheme,
    signature: Vec<u8>,
    chain: Vec<CertificateDer<'static>>,
}

impl Signed {
    fn decode(bytes: &[u8]) -> Result<Self, DescriptorError> {
        if bytes.len() > MAX_DESCRIPTOR_LEN {
            return Err(DescriptorError::TooLong(bytes.len()));
        }
        let wire =
            WireSigned::decode(bytes).map_err(|e| DescriptorError::Malformed(e.to_string()))?;
        if wire.chain.is_empty() || wire.chain.len() > MAX_CHAIN_LEN {
            return Err(DescriptorError::Chain(wire.chain.len()));
        }
        let descriptor = PeerDescriptor::from_body(&wire.body)?;
        let scheme = u16::try_from(wire.scheme)
            .map(SignatureScheme::from)
            .map_err(|_| DescriptorError::Signature)?;
        Ok(Self {
            descriptor,
            message: signed_message(&wire.body),
            scheme,
            signature: wire.signature,
            chain: wire.chain.into_iter().map(CertificateDer::from).collect(),
        })
    }
}

/// What a signature covers: the context, then the body.
fn signed_message(body: &[u8]) -> Vec<u8> {
    [CONTEXT, body].concat()
}

/// Signs the descriptors a destination node serves, with the key of its
/// peer identity (its `[transport]` certificate).
#[derive(Clone)]
pub struct DescriptorSigner {
    key: Arc<CertifiedKey>,
}

impl std::fmt::Debug for DescriptorSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The private key is never printed.
        f.debug_struct("DescriptorSigner").finish_non_exhaustive()
    }
}

impl DescriptorSigner {
    pub(crate) fn new(key: Arc<CertifiedKey>) -> Self {
        Self { key }
    }

    /// `descriptor`, signed, with the node's certificate chain.
    ///
    /// # Errors
    ///
    /// [`DescriptorError::Unsignable`] if the key offers no TLS 1.3
    /// scheme or fails to sign.
    pub fn sign(&self, descriptor: &PeerDescriptor) -> Result<Bytes, DescriptorError> {
        let body = descriptor.body();
        let signer = self
            .key
            .key
            .choose_scheme(SCHEMES)
            .ok_or(DescriptorError::Unsignable)?;
        let signature = signer
            .sign(&signed_message(&body))
            .map_err(|_| DescriptorError::Unsignable)?;
        let wire = WireSigned {
            body,
            scheme: u16::from(signer.scheme()).into(),
            signature,
            chain: self.key.cert.iter().map(|cert| cert.to_vec()).collect(),
        };
        Ok(Bytes::from(wire.encode_to_vec()))
    }
}

/// What a source expects of the descriptor of the bucket it flushes to.
#[derive(Debug, Clone, Copy)]
pub struct Expected<'a> {
    /// The destination bucket.
    pub bucket: &'a BucketName,
    /// The source's own cluster, which the bucket must receive from.
    pub source: &'a ClusterId,
}

/// Verifies descriptors against the peer clusters a node trusts.
#[derive(Debug, Clone)]
pub struct DescriptorVerifier {
    verifier: Arc<PeerVerifier>,
}

impl DescriptorVerifier {
    /// Verifies against `trust`: a descriptor's chain must lead to the CA
    /// bundle of the cluster it names, which must be a configured peer.
    #[must_use]
    pub fn new(trust: Arc<PeerTrust>) -> Self {
        Self {
            verifier: Arc::new(PeerVerifier::new(trust)),
        }
    }

    pub(crate) fn from_verifier(verifier: Arc<PeerVerifier>) -> Self {
        Self { verifier }
    }

    /// The descriptor `bytes` hold, once every check passed at `now`
    /// (time since the Unix epoch): the chain leads to the trust bundle of
    /// the cluster the descriptor names, whose node holds the leaf; the
    /// leaf's key signed the body; `now` is within its validity, give or
    /// take [`DESCRIPTOR_CLOCK_SKEW`] at the start; it is for `expected`'s
    /// bucket and source; and the two ends share a protocol version.
    ///
    /// # Errors
    ///
    /// The [`DescriptorError`] of the first check that fails.
    pub fn verify(
        &self,
        bytes: &[u8],
        now: Duration,
        expected: Expected<'_>,
    ) -> Result<PeerDescriptor, DescriptorError> {
        let signed = Signed::decode(bytes)?;
        let descriptor = &signed.descriptor;
        let (leaf, intermediates) = signed
            .chain
            .split_first()
            .ok_or(DescriptorError::Chain(0))?;
        self.verifier
            .verify(
                leaf,
                intermediates,
                UnixTime::since_unix_epoch(now),
                KeyUsage::server_auth(),
                Some(&descriptor.cluster),
            )
            .map_err(|error| DescriptorError::Untrusted(error.to_string()))?;
        verify_signature(
            self.verifier.algorithms(),
            leaf,
            signed.scheme,
            &signed.message,
            &signed.signature,
        )?;
        if descriptor.issued > now + DESCRIPTOR_CLOCK_SKEW {
            return Err(DescriptorError::NotYetValid);
        }
        if now >= descriptor.expires {
            return Err(DescriptorError::Expired);
        }
        if descriptor.expires - descriptor.issued > MAX_DESCRIPTOR_LIFETIME {
            return Err(DescriptorError::Lifetime);
        }
        if &descriptor.bucket != expected.bucket {
            return Err(DescriptorError::Bucket {
                expected: expected.bucket.clone(),
                found: descriptor.bucket.clone(),
            });
        }
        if &descriptor.source != expected.source {
            return Err(DescriptorError::Source {
                expected: expected.source.clone(),
                found: descriptor.source.clone(),
            });
        }
        let (versions, ours) = (descriptor.versions, SUPPORTED_VERSIONS);
        if versions.min().max(ours.min()) > versions.max().min(ours.max()) {
            return Err(DescriptorError::Versions);
        }
        Ok(signed.descriptor)
    }
}

/// Checks that the key of `leaf` made `signature` over `message` with
/// `scheme`, one of the [`SCHEMES`].
fn verify_signature(
    algorithms: &WebPkiSupportedAlgorithms,
    leaf: &CertificateDer<'_>,
    scheme: SignatureScheme,
    message: &[u8],
    signature: &[u8],
) -> Result<(), DescriptorError> {
    if !SCHEMES.contains(&scheme) {
        return Err(DescriptorError::Signature);
    }
    let cert = EndEntityCert::try_from(leaf).map_err(|_| DescriptorError::Signature)?;
    let verified = algorithms
        .mapping
        .iter()
        .filter(|(mapped, _)| *mapped == scheme)
        .flat_map(|(_, algorithms)| algorithms.iter())
        .any(|algorithm| {
            cert.verify_signature(*algorithm, message, signature)
                .is_ok()
        });
    if verified {
        Ok(())
    } else {
        Err(DescriptorError::Signature)
    }
}

/// The signature verification algorithms of the provider peer
/// connections use.
pub(crate) fn algorithms() -> WebPkiSupportedAlgorithms {
    aws_lc_rs::default_provider().signature_verification_algorithms
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> PeerDescriptor {
        PeerDescriptor::new(
            ClusterId::new("prod-eu").unwrap(),
            BucketName::new("archive").unwrap(),
            ClusterId::new("prod-us").unwrap(),
            vec!["peer.example.internal:7443".to_owned()],
            Duration::from_secs(1_000_000),
        )
    }

    fn envelope(body: Vec<u8>, chain: usize) -> Vec<u8> {
        WireSigned {
            body,
            scheme: 0x0403,
            signature: vec![1; 64],
            chain: vec![vec![0x30]; chain],
        }
        .encode_to_vec()
    }

    #[test]
    fn bodies_read_back_and_bad_ones_are_refused() {
        let descriptor = descriptor();
        let read = PeerDescriptor::decode_unverified(&envelope(descriptor.body(), 1)).unwrap();
        assert_eq!(read, descriptor);
        assert_eq!(read.expires - read.issued, DESCRIPTOR_LIFETIME);

        let refused = |change: &dyn Fn(&mut WireBody)| {
            let mut wire = WireBody::decode(&*descriptor.body()).unwrap();
            change(&mut wire);
            PeerDescriptor::decode_unverified(&envelope(wire.encode_to_vec(), 1)).unwrap_err()
        };
        assert_eq!(refused(&|w| w.format = 2), DescriptorError::Format(2));
        let changes: [&dyn Fn(&mut WireBody); 9] = [
            &|w| w.cluster = "Not A Cluster".to_owned(),
            &|w| w.source = String::new(),
            &|w| w.bucket = "x".to_owned(),
            &|w| w.addresses.clear(),
            &|w| w.addresses = vec!["h:1".to_owned(); MAX_ADVERTISED_ADDRESSES + 1],
            &|w| w.addresses = vec!["no-port".to_owned()],
            &|w| w.min_version = 0,
            &|w| w.max_version = 70_000,
            &|w| w.expires_unix_ms = w.issued_unix_ms,
        ];
        for change in changes {
            assert!(matches!(refused(change), DescriptorError::Malformed(_)));
        }
    }

    #[test]
    fn envelopes_are_bounded() {
        let body = descriptor().body();
        assert_eq!(
            PeerDescriptor::decode_unverified(&envelope(body.clone(), 0)),
            Err(DescriptorError::Chain(0))
        );
        assert_eq!(
            PeerDescriptor::decode_unverified(&envelope(body, MAX_CHAIN_LEN + 1)),
            Err(DescriptorError::Chain(MAX_CHAIN_LEN + 1))
        );
        let long = vec![0; MAX_DESCRIPTOR_LEN + 1];
        assert_eq!(
            PeerDescriptor::decode_unverified(&long),
            Err(DescriptorError::TooLong(MAX_DESCRIPTOR_LEN + 1))
        );
        assert!(matches!(
            PeerDescriptor::decode_unverified(&[0xff; 8]),
            Err(DescriptorError::Malformed(_))
        ));
    }
}
