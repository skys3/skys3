//! A node's credentials from the operator's PKI, and the certificate
//! verification both ends of a connection apply.
//!
//! Verification runs inside the TLS handshake, on both sides:
//!
//! - The chain must lead to one of the configured CA certificates, every
//!   certificate in it must be within its validity period, and the leaf
//!   must allow its use (the `clientAuth` or `serverAuth` extended key
//!   usage, when it lists any).
//! - The leaf must carry exactly one SPIFFE ID of this cluster
//!   ([`PeerIdentity`]).
//! - A server must be a node: operator tools only connect.
//!
//! TLS 1.3 is the only version, with the `aws-lc-rs` provider that the rest
//! of the binary uses (design §15). Revocation lists are not checked; the
//! operator's PKI should issue short-lived certificates.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, aws_lc_rs};
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, TrustAnchor, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, OtherError,
    ServerConfig, SignatureScheme,
};
use skys3_types::ClusterId;
use webpki::{EndEntityCert, KeyUsage};

use crate::ALPN_PROTOCOL;
use crate::identity::{IdentityError, PeerIdentity, Role};

/// Why credentials could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PkiError {
    /// A file could not be read.
    #[error("cannot read {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// Why it could not be read.
        source: io::Error,
    },
    /// A PEM file has no usable item of the kind it should hold.
    #[error("invalid PEM in the {what}: {source}")]
    Pem {
        /// Which input: the certificate chain, the private key, or the CA
        /// bundle.
        what: &'static str,
        /// What is wrong with it.
        source: pem::Error,
    },
    /// The certificate chain is empty.
    #[error("the certificate chain is empty")]
    NoCertificate,
    /// The CA bundle has no certificate.
    #[error("the CA bundle has no certificate")]
    NoCa,
    /// A certificate cannot be parsed.
    #[error("invalid certificate in the {what}: {source}")]
    Certificate {
        /// Which input: the certificate chain or the CA bundle.
        what: &'static str,
        /// What is wrong with it.
        source: webpki::Error,
    },
    /// The node certificate's identity is not one of this cluster.
    #[error("the certificate's identity is not usable: {0}")]
    Identity(#[from] IdentityError),
    /// The private key cannot be used or does not match the certificate.
    #[error("the private key is not usable with the certificate: {0}")]
    Key(rustls::Error),
}

/// A holder's certificate chain, private key, and the CA certificates it
/// trusts, for one cluster.
pub struct Credentials {
    identity: PeerIdentity,
    certified: Arc<CertifiedKey>,
    verifier: Arc<PeerVerifier>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The private key is never printed.
        f.debug_struct("Credentials")
            .field("cluster", &self.verifier.cluster)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// Checks and assembles credentials from DER: the certificate chain,
    /// leaf first, its private key, and the CA certificates peers' chains
    /// must lead to.
    ///
    /// The leaf's identity must be one of `cluster`, and the key must match
    /// it. The chain itself is verified by peers, so a certificate that has
    /// expired or that the CA did not issue loads here but fails every
    /// handshake.
    ///
    /// # Errors
    ///
    /// [`PkiError`] naming the input that is unusable.
    pub fn new(
        cluster: ClusterId,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        ca: &[CertificateDer<'_>],
    ) -> Result<Self, PkiError> {
        let leaf = chain.first().ok_or(PkiError::NoCertificate)?;
        let leaf = EndEntityCert::try_from(leaf).map_err(|source| PkiError::Certificate {
            what: "certificate chain",
            source,
        })?;
        let identity = PeerIdentity::from_uri_names(leaf.valid_uri_names(), &cluster)?;
        let roots = ca
            .iter()
            .map(|cert| webpki::anchor_from_trusted_cert(cert).map(|anchor| anchor.to_owned()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| PkiError::Certificate {
                what: "CA bundle",
                source,
            })?;
        if roots.is_empty() {
            return Err(PkiError::NoCa);
        }
        let provider = provider();
        let certified = CertifiedKey::from_der(chain, key, &provider).map_err(PkiError::Key)?;
        Ok(Self {
            identity,
            certified: Arc::new(certified),
            verifier: Arc::new(PeerVerifier {
                cluster,
                roots,
                algorithms: provider.signature_verification_algorithms,
            }),
        })
    }

    /// Parses credentials from PEM: a certificate chain, leaf first, a
    /// private key (PKCS #8, PKCS #1, or SEC1), and a CA bundle.
    ///
    /// # Errors
    ///
    /// [`PkiError::Pem`] for malformed PEM, and the errors of
    /// [`Credentials::new`].
    pub fn from_pem(
        cluster: ClusterId,
        cert_pem: &[u8],
        key_pem: &[u8],
        ca_pem: &[u8],
    ) -> Result<Self, PkiError> {
        let pem_error = |what| move |source| PkiError::Pem { what, source };
        let chain = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(pem_error("certificate chain"))?;
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(pem_error("private key"))?;
        let ca = CertificateDer::pem_slice_iter(ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(pem_error("CA bundle"))?;
        Self::new(cluster, chain, key, &ca)
    }

    /// Reads credentials from PEM files, as [`Credentials::from_pem`]. The
    /// files are read with blocking I/O, once at startup.
    ///
    /// # Errors
    ///
    /// [`PkiError::Read`] for a file that cannot be read, and the errors of
    /// [`Credentials::from_pem`].
    pub fn load(
        cluster: ClusterId,
        cert_file: &Path,
        key_file: &Path,
        ca_file: &Path,
    ) -> Result<Self, PkiError> {
        let read = |path: &Path| {
            std::fs::read(path).map_err(|source| PkiError::Read {
                path: path.to_owned(),
                source,
            })
        };
        let key = zeroize::Zeroizing::new(read(key_file)?);
        Self::from_pem(cluster, &read(cert_file)?, &key, &read(ca_file)?)
    }

    /// The identity the certificate binds.
    #[must_use]
    pub fn identity(&self) -> &PeerIdentity {
        &self.identity
    }

    /// The cluster the credentials belong to.
    #[must_use]
    pub fn cluster(&self) -> &ClusterId {
        &self.verifier.cluster
    }

    /// The certificate chain and signing key, for a TLS configuration of
    /// another transport with its own trust rules, such as the peer
    /// transport between clusters.
    #[must_use]
    pub fn certified_key(&self) -> Arc<CertifiedKey> {
        self.certified.clone()
    }

    /// The TLS configuration for accepting connections. Only nodes serve.
    pub(crate) fn server_config(&self) -> Option<Arc<ServerConfig>> {
        if self.identity.role() != Role::Node {
            return None;
        }
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .with_client_cert_verifier(self.verifier.clone())
            .with_cert_resolver(Arc::new(SingleCertAndKey::from(self.certified.clone())));
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        // No resumption: every connection presents and verifies a full
        // certificate chain, so an expired or replaced certificate is never
        // accepted from a ticket.
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        Some(Arc::new(config))
    }

    /// The TLS configuration for connecting to nodes.
    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        let mut config = ClientConfig::builder_with_provider(Arc::new(provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("the provider supports TLS 1.3")
            .dangerous()
            .with_custom_certificate_verifier(self.verifier.clone())
            .with_client_cert_resolver(Arc::new(SingleCertAndKey::from(self.certified.clone())));
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        // Peers are named by the SPIFFE ID in their certificates, not by a
        // host name, so no name is sent.
        config.enable_sni = false;
        Arc::new(config)
    }
}

/// The cryptographic provider of every connection.
fn provider() -> CryptoProvider {
    aws_lc_rs::default_provider()
}

/// The identity in a leaf certificate that has been verified already.
pub(crate) fn identity_of(
    leaf: &CertificateDer<'_>,
    cluster: &ClusterId,
) -> Result<PeerIdentity, rustls::Error> {
    let leaf = EndEntityCert::try_from(leaf).map_err(pki_error)?;
    PeerIdentity::from_uri_names(leaf.valid_uri_names(), cluster).map_err(identity_error)
}

/// Verifies peer certificates against the cluster's CA and identity rules.
#[derive(Debug)]
struct PeerVerifier {
    cluster: ClusterId,
    roots: Vec<TrustAnchor<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl PeerVerifier {
    fn verify(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
        usage: KeyUsage,
    ) -> Result<PeerIdentity, rustls::Error> {
        let cert = EndEntityCert::try_from(leaf).map_err(pki_error)?;
        cert.verify_for_usage(
            self.algorithms.all,
            &self.roots,
            intermediates,
            now,
            usage,
            None,
            None,
        )
        .map_err(pki_error)?;
        PeerIdentity::from_uri_names(cert.valid_uri_names(), &self.cluster).map_err(identity_error)
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
        self.verify(leaf, intermediates, now, KeyUsage::client_auth())?;
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
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let identity = self.verify(leaf, intermediates, now, KeyUsage::server_auth())?;
        if identity.role() != Role::Node {
            return Err(identity_error(IdentityError::WrongRole {
                found: identity.role(),
                expected: Role::Node,
            }));
        }
        // The caller checks that the node is the one it meant to reach.
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

fn identity_error(error: IdentityError) -> rustls::Error {
    CertificateError::Other(OtherError(Arc::new(error))).into()
}

/// Maps a path-building error to the TLS error, and so the alert, rustls
/// would use for it. rustls keeps its own mapping private.
pub fn pki_error(error: webpki::Error) -> rustls::Error {
    use webpki::Error as E;
    let error = match error {
        E::BadDer | E::BadDerTime | E::TrailingData(_) => CertificateError::BadEncoding,
        E::CertExpired { .. } | E::InvalidCertValidity => CertificateError::Expired,
        E::CertNotValidYet { .. } => CertificateError::NotValidYet,
        E::UnknownIssuer => CertificateError::UnknownIssuer,
        E::InvalidSignatureForPublicKey => CertificateError::BadSignature,
        E::RequiredEkuNotFoundContext(_) => CertificateError::InvalidPurpose,
        E::UnsupportedCriticalExtension => CertificateError::UnhandledCriticalExtension,
        other => CertificateError::Other(OtherError(Arc::new(other))),
    };
    error.into()
}
