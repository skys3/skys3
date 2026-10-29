//! Throwaway PKIs for peer endpoint tests: one CA per cluster, generated
//! with `rcgen` when a test runs, and raw QUIC clients and servers that
//! break the rules the endpoint enforces.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use skys3_config::{BucketPair, CongestionControl};
use skys3_net::Credentials;
use skys3_peer::{
    ALPN, Capabilities, Destination, EndpointSettings, Hello, Message, PeerConnection,
    PeerEndpoint, PeerTls, PeerTrust, SUPPORTED_VERSIONS,
};
use skys3_types::{BucketId, BucketName, ClusterId};

/// The longest a test waits on the network before it fails.
pub const WAIT: Duration = Duration::from_secs(30);

/// Bounds a network wait, so a test that would hang fails instead.
pub trait Bounded: Future + Sized {
    /// Waits at most [`WAIT`] for the future, panicking after that.
    #[track_caller]
    fn bounded(self) -> impl Future<Output = Self::Output> {
        let caller = std::panic::Location::caller();
        async move {
            tokio::time::timeout(WAIT, self)
                .await
                .unwrap_or_else(|_| panic!("a network wait at {caller} took over {WAIT:?}"))
        }
    }
}

impl<F: Future> Bounded for F {}

pub fn id(cluster: &str) -> ClusterId {
    ClusterId::new(cluster).unwrap()
}

pub fn pair(source: &str, destination: &str) -> BucketPair {
    BucketPair {
        source: BucketId::new(source).unwrap(),
        destination: BucketName::new(destination).unwrap(),
    }
}

/// A leaf certificate and its key.
pub struct Issued {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

/// A cluster's certificate authority.
pub struct Ca {
    issuer: Issuer<'static, KeyPair>,
    cert: CertificateDer<'static>,
}

impl Ca {
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

    /// A current certificate for client and server use naming `uri`.
    pub fn issue(&self, uri: &str) -> Issued {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, "leaf");
        params.subject_alt_names = vec![SanType::URI(uri.try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        (params.not_before, params.not_after) =
            (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1));
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        Issued {
            cert,
            key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        }
    }

    /// The node certificate of `node` in `cluster`.
    pub fn node(&self, cluster: &str, node: &str) -> Issued {
        self.issue(&format!("spiffe://{cluster}/node/{node}"))
    }

    /// Node credentials of `cluster`, issued by this CA.
    pub fn credentials(&self, cluster: &str, node: &str) -> Credentials {
        let issued = self.node(cluster, node);
        Credentials::new(id(cluster), vec![issued.cert], issued.key, &[self.cert()]).unwrap()
    }
}

/// A cluster: its ID and CA.
pub struct Cluster {
    pub id: &'static str,
    pub ca: Ca,
}

impl Cluster {
    pub fn new(id: &'static str) -> Self {
        Self {
            id,
            ca: Ca::new(id),
        }
    }

    /// Trusts `self` for `pairs`.
    pub fn trusted(&self, trust: &mut PeerTrust, pairs: &[BucketPair]) {
        trust
            .add(id(self.id), &[self.ca.cert()], pairs.iter().cloned())
            .unwrap();
    }

    /// The TLS identity of node `node`, trusting `trust`.
    pub fn tls(&self, node: &str, trust: PeerTrust) -> PeerTls {
        PeerTls::new(&self.ca.credentials(self.id, node), Arc::new(trust)).unwrap()
    }

    /// An endpoint of node `node` on loopback.
    pub fn endpoint(&self, node: &str, trust: PeerTrust) -> PeerEndpoint {
        PeerEndpoint::bind(loopback(), &self.tls(node, trust), settings()).unwrap()
    }
}

pub fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

pub fn settings() -> EndpointSettings {
    EndpointSettings {
        congestion_control: CongestionControl::Cubic,
        connect_timeout: Duration::from_secs(5),
        max_inflight_bytes: 64 << 20,
        capabilities: Capabilities::KNOWN,
    }
}

/// `endpoint` as a destination of `cluster`.
pub fn destination(cluster: &str, endpoint: &PeerEndpoint) -> Destination {
    Destination {
        cluster: id(cluster),
        address: endpoint.local_addr().unwrap(),
    }
}

/// Accepts the next connection at `endpoint`.
pub async fn accept(endpoint: &PeerEndpoint) -> Result<PeerConnection, skys3_peer::ConnectError> {
    endpoint
        .accept()
        .bounded()
        .await
        .unwrap()
        .establish()
        .bounded()
        .await
}

/// A `HELLO` of `cluster`.
pub fn hello(cluster: &str) -> Message {
    Message::Hello(Hello {
        cluster: id(cluster),
        versions: SUPPORTED_VERSIONS,
        capabilities: Capabilities::KNOWN,
    })
}

/// A server verifier that accepts any certificate, for raw clients that
/// test the endpoint's own checks.
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
        aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A QUIC client that presents `issued`, trusts any server, and keeps
/// session tickets and sends 0-RTT data whenever it can.
pub struct RawClient {
    pub endpoint: quinn::Endpoint,
    pub config: quinn::ClientConfig,
}

impl RawClient {
    pub fn new(issued: Issued) -> Self {
        let mut tls =
            rustls::ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServer))
                .with_client_auth_cert(vec![issued.cert], issued.key)
                .unwrap();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        tls.enable_early_data = true;
        let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
        Self {
            endpoint: quinn::Endpoint::client(loopback()).unwrap(),
            config,
        }
    }

    /// Starts a connection to `address`, naming the server `server_name`.
    pub fn connect(&self, address: SocketAddr, server_name: &str) -> quinn::Connecting {
        self.endpoint
            .connect_with(self.config.clone(), address, server_name)
            .unwrap()
    }
}

/// Sends `message` on a new stream of `connection`, finishes it, and
/// returns the peer's reply on it, if any.
pub async fn round_trip(
    connection: &quinn::Connection,
    message: &Message,
) -> Result<Option<Message>, Box<dyn std::error::Error>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&message.encode()?).await?;
    send.finish()?;
    let bytes = recv.read_to_end(1 << 20).await?;
    Ok(Message::decode(&bytes)?.map(|(message, _)| message))
}

/// A QUIC server that presents `issued`, asks for no client certificate,
/// issues session tickets, and accepts 0-RTT data: everything the peer
/// endpoint refuses.
pub fn ticket_server(issued: Issued) -> quinn::Endpoint {
    let mut tls =
        rustls::ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![issued.cert], issued.key)
            .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.max_early_data_size = u32::MAX;
    let config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap()));
    quinn::Endpoint::server(config, loopback()).unwrap()
}
