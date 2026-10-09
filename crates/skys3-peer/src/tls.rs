//! Mutual TLS between clusters (design §12): each end presents its node
//! certificate and verifies the other's against the trust bundle of the
//! peer cluster the certificate names.
//!
//! - TLS 1.3 only, on the `aws-lc-rs` provider the rest of the binary
//!   uses (§15).
//! - The peer's chain must lead to the CA bundle of the cluster its SPIFFE
//!   ID names, that cluster must be a trusted peer, and the holder must be
//!   a node. A client also checks that the server belongs to the cluster
//!   it meant to reach.
//! - No session resumption and no 0-RTT. A server issues no tickets and
//!   accepts no early data, and a client neither stores tickets nor sends
//!   early data, so a replayed peer message can never apply a mutation and
//!   every connection verifies a full chain.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, aws_lc_rs};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, OtherError,
    ServerConfig, SignatureScheme,
};
use skys3_net::{Credentials, IdentityError, PeerIdentity, Role, pki_error};
use skys3_types::{ClusterId, NodeId};
use webpki::{EndEntityCert, KeyUsage};

use crate::descriptor::{DescriptorSigner, DescriptorVerifier};
use crate::trust::PeerTrust;

/// The ALPN protocol of peer connections. It names the protocol, not a
/// version: the frame format and `HELLO` never change, and `HELLO`
/// negotiates the version of everything after it.
pub const ALPN: &[u8] = b"skys3-peer";

/// The URI scheme of a SPIFFE ID.
const SPIFFE_SCHEME: &str = "spiffe://";

/// Why a certificate was refused, beyond the errors of path building.
#[derive(Debug, thiserror::Error)]
enum PeerCertError {
    #[error("the certificate names no trust domain that is a cluster ID")]
    NoTrustDomain,
    #[error("the certificate names cluster {found}, expected {expected}")]
    WrongCluster {
        found: ClusterId,
        expected: ClusterId,
    },
    #[error("the server name {0:?} is not a cluster ID")]
    ServerName(String),
    #[error(transparent)]
    Identity(#[from] IdentityError),
}

impl From<PeerCertError> for rustls::Error {
    fn from(error: PeerCertError) -> Self {
        CertificateError::Other(OtherError(Arc::new(error))).into()
    }
}

/// A node's TLS identity for peer connections: its own certificate from
/// its cluster's PKI, and the peer clusters it trusts.
pub struct PeerTls {
    cluster: ClusterId,
    certified: Arc<CertifiedKey>,
    verifier: Arc<PeerVerifier>,
}

impl std::fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The private key is never printed.
        f.debug_struct("PeerTls")
            .field("cluster", &self.cluster)
            .field("trust", &self.verifier.trust)
            .finish_non_exhaustive()
    }
}

impl PeerTls {
    /// The TLS identity of a node with `credentials` (its `[transport]`
    /// certificate) that trusts the peers of `trust`. Returns `None` for
    /// an operator tool's credentials: only nodes peer.
    #[must_use]
    pub fn new(credentials: &Credentials, trust: Arc<PeerTrust>) -> Option<Self> {
        if credentials.identity().role() != Role::Node {
            return None;
        }
        Some(Self {
            cluster: credentials.cluster().clone(),
            certified: credentials.certified_key(),
            verifier: Arc::new(PeerVerifier::new(trust)),
        })
    }

    /// Signs the peer descriptors this node serves (§7.8), with the key of
    /// the certificate it presents to peers.
    #[must_use]
    pub fn descriptor_signer(&self) -> DescriptorSigner {
        DescriptorSigner::new(Arc::clone(&self.certified))
    }

    /// Verifies the peer descriptors of the clusters this node trusts.
    #[must_use]
    pub fn descriptor_verifier(&self) -> DescriptorVerifier {
        DescriptorVerifier::from_verifier(Arc::clone(&self.verifier))
    }

    /// This node's cluster.
    #[must_use]
    pub fn cluster(&self) -> &ClusterId {
        &self.cluster
    }

    /// The peers this node trusts.
    #[must_use]
    pub fn trust(&self) -> &Arc<PeerTrust> {
        &self.verifier.trust
    }

    /// The configuration for accepting peer connections.
    #[must_use]
    pub fn server_config(&self) -> ServerConfig {
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .with_client_cert_verifier(self.verifier.clone())
            .with_cert_resolver(Arc::new(SingleCertAndKey::from(self.certified.clone())));
        config.alpn_protocols = vec![ALPN.to_vec()];
        // No 0-RTT: QUIC allows only 0 or u32::MAX, and 0 refuses early
        // data. No tickets either, so there is nothing to resume.
        config.max_early_data_size = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        config
    }

    /// The configuration for connecting to peers. The server name given
    /// to the handshake must be the cluster ID of the peer meant.
    #[must_use]
    pub fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::builder_with_provider(Arc::new(provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .dangerous()
            .with_custom_certificate_verifier(self.verifier.clone())
            .with_client_cert_resolver(Arc::new(SingleCertAndKey::from(self.certified.clone())));
        config.alpn_protocols = vec![ALPN.to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        config.enable_early_data = false;
        // Peers are named by the SPIFFE ID in their certificates, and the
        // server name only carries the expected cluster to the verifier.
        config.enable_sni = false;
        config
    }
}

/// The cryptographic provider of every peer connection.
fn provider() -> CryptoProvider {
    aws_lc_rs::default_provider()
}

/// The cluster a certificate's first SPIFFE ID names as its trust domain.
/// [`PeerIdentity::from_uri_names`] then requires exactly one SPIFFE ID.
fn trust_domain<'a>(mut uris: impl Iterator<Item = &'a str>) -> Option<ClusterId> {
    let uri = uris.find(|uri| uri.starts_with(SPIFFE_SCHEME))?;
    let domain = uri[SPIFFE_SCHEME.len()..].split('/').next()?;
    ClusterId::new(domain).ok()
}

/// The cluster and node a verified leaf certificate names.
pub(crate) fn peer_of(leaf: &CertificateDer<'_>) -> Option<(ClusterId, NodeId)> {
    let cert = EndEntityCert::try_from(leaf).ok()?;
    let cluster = trust_domain(cert.valid_uri_names())?;
    let node = PeerIdentity::from_uri_names(cert.valid_uri_names(), &cluster)
        .ok()?
        .node_id()?
        .clone();
    Some((cluster, node))
}

/// Verifies peer certificates against the trust bundle of the cluster
/// each one names.
#[derive(Debug)]
pub(crate) struct PeerVerifier {
    trust: Arc<PeerTrust>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl PeerVerifier {
    pub(crate) fn new(trust: Arc<PeerTrust>) -> Self {
        Self {
            trust,
            algorithms: crate::descriptor::algorithms(),
        }
    }

    /// The signature verification algorithms it accepts.
    pub(crate) fn algorithms(&self) -> &WebPkiSupportedAlgorithms {
        &self.algorithms
    }

    /// Verifies `leaf`'s chain for `usage` at `now`: it must lead to the
    /// CA bundle of the peer cluster it names (`expected`, if given), and
    /// name a node.
    pub(crate) fn verify(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
        usage: KeyUsage,
        expected: Option<&ClusterId>,
    ) -> Result<(), rustls::Error> {
        let cert = EndEntityCert::try_from(leaf).map_err(pki_error)?;
        let cluster = trust_domain(cert.valid_uri_names()).ok_or(PeerCertError::NoTrustDomain)?;
        if let Some(expected) = expected
            && &cluster != expected
        {
            return Err(PeerCertError::WrongCluster {
                found: cluster,
                expected: expected.clone(),
            }
            .into());
        }
        // An unknown cluster has no CA, as an unknown issuer has none.
        let roots = self
            .trust
            .roots(&cluster)
            .ok_or(CertificateError::UnknownIssuer)?;
        cert.verify_for_usage(
            self.algorithms.all,
            roots,
            intermediates,
            now,
            usage,
            None,
            None,
        )
        .map_err(pki_error)?;
        let identity = PeerIdentity::from_uri_names(cert.valid_uri_names(), &cluster)
            .map_err(PeerCertError::Identity)?;
        if identity.role() != Role::Node {
            return Err(PeerCertError::Identity(IdentityError::WrongRole {
                found: identity.role(),
                expected: Role::Node,
            })
            .into());
        }
        Ok(())
    }
}

impl ClientCertVerifier for PeerVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.verify(leaf, intermediates, now, KeyUsage::client_auth(), None)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let name = server_name.to_str();
        let expected =
            ClusterId::new(name.as_ref()).map_err(|_| PeerCertError::ServerName(name.into()))?;
        self.verify(
            leaf,
            intermediates,
            now,
            KeyUsage::server_auth(),
            Some(&expected),
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_trust_domain_is_the_first_spiffe_id() {
        let domain = trust_domain(
            ["https://example.com/x", "spiffe://prod-eu/node/n1"]
                .iter()
                .copied(),
        );
        assert_eq!(domain.unwrap().as_str(), "prod-eu");
        assert_eq!(trust_domain(["https://prod-eu/node"].iter().copied()), None);
        assert_eq!(
            trust_domain(["spiffe://Prod/node/n1"].iter().copied()),
            None
        );
        assert_eq!(trust_domain(["spiffe://"].iter().copied()), None);
    }

    #[test]
    fn refusals_explain_themselves() {
        let error: rustls::Error = PeerCertError::WrongCluster {
            found: ClusterId::new("a").unwrap(),
            expected: ClusterId::new("b").unwrap(),
        }
        .into();
        assert!(error.to_string().contains("WrongCluster"), "{error}");
        let error: rustls::Error = PeerCertError::ServerName("10.0.0.1".into()).into();
        assert!(error.to_string().contains("10.0.0.1"), "{error}");
    }
}
