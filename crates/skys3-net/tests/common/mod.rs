//! A throwaway PKI for transport tests: CAs and leaf certificates generated
//! with `rcgen` when a test runs, so no private key is ever committed.

#![allow(dead_code)]

use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use skys3_net::{CertificateDer, Credentials, PrivateKeyDer};
use skys3_types::ClusterId;

/// The cluster every test uses unless it tests another.
pub const CLUSTER: &str = "test-cluster";

pub fn cluster() -> ClusterId {
    ClusterId::new(CLUSTER).unwrap()
}

/// When a certificate is valid.
#[derive(Clone, Copy, Debug)]
pub enum Validity {
    /// From 2000 to 4000.
    Current,
    /// Expired in 2001.
    Expired,
    /// Valid from 4000.
    NotYetValid,
}

/// The options of a leaf certificate.
#[derive(Clone, Debug)]
pub struct Leaf {
    pub uris: Vec<String>,
    pub validity: Validity,
    pub client_auth: bool,
    pub server_auth: bool,
}

impl Leaf {
    /// A current certificate for both client and server use with one URI.
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uris: vec![uri.into()],
            validity: Validity::Current,
            client_auth: true,
            server_auth: true,
        }
    }

    pub fn node(name: &str) -> Self {
        Self::new(format!("spiffe://{CLUSTER}/node/{name}"))
    }

    pub fn admin(name: &str) -> Self {
        Self::new(format!("spiffe://{CLUSTER}/admin/{name}"))
    }

    pub fn validity(mut self, validity: Validity) -> Self {
        self.validity = validity;
        self
    }
}

/// A certificate authority.
pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    cert: CertificateDer<'static>,
}

/// A leaf certificate and its key.
pub struct Issued {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

impl TestCa {
    pub fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).unwrap().der().clone();
        Self {
            issuer: Issuer::new(params, key),
            cert,
        }
    }

    pub fn cert(&self) -> CertificateDer<'static> {
        self.cert.clone()
    }

    pub fn issue(&self, leaf: &Leaf) -> Issued {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "skys3 test leaf");
        params.subject_alt_names = leaf
            .uris
            .iter()
            .map(|uri| SanType::URI(uri.as_str().try_into().unwrap()))
            .collect();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        if leaf.client_auth {
            params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ClientAuth);
        }
        if leaf.server_auth {
            params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        (params.not_before, params.not_after) = match leaf.validity {
            Validity::Current => (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1)),
            Validity::Expired => (date_time_ymd(2000, 1, 1), date_time_ymd(2001, 1, 1)),
            Validity::NotYetValid => (date_time_ymd(4000, 1, 1), date_time_ymd(4001, 1, 1)),
        };
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        Issued {
            cert,
            key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        }
    }

    /// Credentials of `cluster` for `leaf`, trusting this CA.
    pub fn credentials_in(&self, cluster: ClusterId, leaf: &Leaf) -> Credentials {
        let issued = self.issue(leaf);
        Credentials::new(cluster, vec![issued.cert], issued.key, &[self.cert()]).unwrap()
    }

    /// Credentials of the test cluster for `leaf`, trusting this CA.
    pub fn credentials(&self, leaf: &Leaf) -> Credentials {
        self.credentials_in(cluster(), leaf)
    }
}

/// A PEM block of `label` holding `der`.
pub fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// A server verifier that accepts any certificate, for raw clients that
/// test the server's checks.
#[derive(Debug)]
pub struct AcceptAnyServer;

impl ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A raw TLS 1.3 client config: an optional client certificate, any
/// server accepted, and the given ALPN protocols.
pub fn raw_client(cert: Option<Issued>, alpn: &[&[u8]]) -> Arc<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnyServer));
    let mut config = match cert {
        Some(issued) => builder
            .with_client_auth_cert(vec![issued.cert], issued.key)
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// A raw TLS 1.3 server config presenting `issued`, without client
/// authentication.
pub fn raw_server(issued: Issued, alpn: &[&[u8]]) -> Arc<rustls::ServerConfig> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![issued.cert], issued.key)
    .unwrap();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}
