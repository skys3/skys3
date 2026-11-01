//! A throwaway PKI for the simulated cluster: a CA and one certificate per
//! node, generated with `rcgen` when the cluster is built, so no private
//! key is ever committed.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, PKCS_ED25519, SanType, SerialNumber,
    SignatureAlgorithm, date_time_ymd,
};
use skys3_net::{CertificateDer, Credentials, PeerIdentity, PrivateKeyDer};
use skys3_types::{ClusterId, NodeId};

/// The cluster's certificate authority.
pub(crate) struct Pki {
    issuer: Issuer<'static, KeyPair>,
    cert: CertificateDer<'static>,
    /// The algorithm of every key, the CA's included.
    algorithm: &'static SignatureAlgorithm,
    /// The serial number of the next certificate.
    serial: std::sync::atomic::AtomicU64,
}

impl Pki {
    /// A new CA, with ECDSA keys.
    pub(crate) fn new() -> Result<Self, rcgen::Error> {
        Self::with_algorithm(&PKCS_ECDSA_P256_SHA256)
    }

    /// A new CA with Ed25519 keys, whose signatures and certificates have
    /// the same size in every run: what they sign over the simulated
    /// network then takes as many bytes whatever the keys drawn.
    pub(crate) fn ed25519() -> Result<Self, rcgen::Error> {
        Self::with_algorithm(&PKCS_ED25519)
    }

    fn with_algorithm(algorithm: &'static SignatureAlgorithm) -> Result<Self, rcgen::Error> {
        let key = KeyPair::generate_for(algorithm)?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "skys3 simulation CA");
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.serial_number = Some(SerialNumber::from(1u64));
        let cert = params.self_signed(&key)?.der().clone();
        Ok(Self {
            issuer: Issuer::new(params, key),
            cert,
            algorithm,
            serial: std::sync::atomic::AtomicU64::new(2),
        })
    }

    /// The CA's certificate.
    pub(crate) fn ca(&self) -> &CertificateDer<'static> {
        &self.cert
    }

    /// Credentials for `node` of `cluster`: a certificate naming its
    /// SPIFFE ID (design §12), valid for client and server use.
    pub(crate) fn node(
        &self,
        cluster: &ClusterId,
        node: &NodeId,
    ) -> Result<Credentials, Box<dyn std::error::Error + Send + Sync>> {
        let identity = PeerIdentity::Node(node.clone()).spiffe_id(cluster);
        let key = KeyPair::generate_for(self.algorithm)?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params
            .distinguished_name
            .push(DnType::CommonName, node.as_str());
        params.subject_alt_names = vec![SanType::URI(identity.as_str().try_into()?)];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let serial = self
            .serial
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        params.serial_number = Some(SerialNumber::from(serial));
        (params.not_before, params.not_after) =
            (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1));
        let cert = params.signed_by(&key, &self.issuer)?.der().clone();
        let key = PrivateKeyDer::try_from(key.serialize_der())?;
        let credentials = Credentials::new(
            cluster.clone(),
            vec![cert],
            key,
            std::slice::from_ref(&self.cert),
        )?;
        Ok(credentials)
    }
}
