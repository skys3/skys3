//! A throwaway PKI for tests that talk over the real transport.

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_net::{
    CertificateDer, Credentials, PeerIdentity, PrivateKeyDer, TokioNetwork, Transport,
};
use skys3_types::{ClusterId, Label, NodeId};

/// A cluster CA that issues node and operator-tool certificates.
pub(crate) struct Pki {
    pub(crate) cluster: ClusterId,
    issuer: Issuer<'static, KeyPair>,
    ca: CertificateDer<'static>,
}

impl Pki {
    pub(crate) fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = params.self_signed(&key).unwrap().der().clone();
        Self {
            cluster: ClusterId::new("test").unwrap(),
            issuer: Issuer::new(params, key),
            ca,
        }
    }

    /// The transport of the node `node`.
    pub(crate) fn transport(&self, node: &NodeId) -> Transport<TokioNetwork> {
        self.issue(&PeerIdentity::Node(node.clone()))
    }

    /// The transport of an operator tool named `name`.
    pub(crate) fn tool(&self, name: &str) -> Transport<TokioNetwork> {
        self.issue(&PeerIdentity::Admin(Label::new(name).unwrap()))
    }

    fn issue(&self, holder: &PeerIdentity) -> Transport<TokioNetwork> {
        let identity = holder.spiffe_id(&self.cluster);
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![SanType::URI(identity.as_str().try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        let key = PrivateKeyDer::try_from(key.serialize_der()).unwrap();
        let credentials = Credentials::new(
            self.cluster.clone(),
            vec![cert],
            key,
            std::slice::from_ref(&self.ca),
        )
        .unwrap();
        Transport::new(TokioNetwork, &credentials)
    }
}
