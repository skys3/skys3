//! Clusters of nodes over a shared control store (plan M8-04 on):
//! a throwaway PKI, the `[control_store]` of the in-process stand-in for
//! etcd, and the configuration of a node of the cluster.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use skys3_control::etcd_fake::Fake;
use skys3_net::Credentials;
use skys3_types::ClusterId;

use super::config_text;

/// The cluster of `config_text`.
pub const CLUSTER: &str = "node-test";

/// How long a test waits on the network before it fails.
pub const WAIT: Duration = Duration::from_secs(30);

/// `der` as a PEM block labeled `label`.
pub fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let text = base64::engine::general_purpose::STANDARD.encode(der);
    let lines: Vec<&str> = text
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

/// A throwaway CA.
pub struct Ca {
    pub issuer: Issuer<'static, KeyPair>,
    pub der: CertificateDer<'static>,
}

/// A certificate chain and its key.
pub struct Leaf {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivatePkcs8KeyDer<'static>,
}

impl Ca {
    pub fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).unwrap();
        Self {
            der: cert.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A node certificate naming `spiffe://<cluster>/node/<node>`, also
    /// valid for `localhost`, so that a plain TLS client can verify it.
    pub fn node(&self, cluster: &str, node: &str) -> Leaf {
        self.leaf(
            &format!("spiffe://{cluster}/node/{node}"),
            vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ],
        )
    }

    /// An operator tool's certificate naming
    /// `spiffe://<cluster>/admin/<name>`, for clients only.
    pub fn tool(&self, cluster: &str, name: &str) -> Leaf {
        self.leaf(
            &format!("spiffe://{cluster}/admin/{name}"),
            vec![ExtendedKeyUsagePurpose::ClientAuth],
        )
    }

    /// A certificate naming `uri`, with `usages`.
    fn leaf(&self, uri: &str, usages: Vec<ExtendedKeyUsagePurpose>) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![
            SanType::URI(uri.try_into().unwrap()),
            SanType::DnsName("localhost".try_into().unwrap()),
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = usages;
        (params.not_before, params.not_after) =
            (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1));
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            chain: vec![cert.der().clone()],
            key: PrivatePkcs8KeyDer::from(key.serialize_der()),
        }
    }
}

impl Leaf {
    /// Writes the chain and key into `dir` as `<name>.pem` and
    /// `<name>.key`.
    pub fn write(&self, dir: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = dir.join(format!("{name}.pem"));
        let key = dir.join(format!("{name}.key"));
        std::fs::write(&cert, pem("CERTIFICATE", &self.chain[0])).unwrap();
        std::fs::write(&key, pem("PRIVATE KEY", self.key.secret_pkcs8_der())).unwrap();
        (cert, key)
    }

    /// Transport credentials of `cluster` trusting `ca`.
    pub fn credentials(&self, cluster: &str, ca: &Ca) -> Credentials {
        Credentials::new(
            ClusterId::new(cluster).unwrap(),
            self.chain.clone(),
            PrivateKeyDer::Pkcs8(self.key.clone_key()),
            std::slice::from_ref(&ca.der),
        )
        .unwrap()
    }
}

/// The cluster's CA, a second cluster's CA that the nodes also trust (a CA
/// shared by two clusters), and a CA nobody trusts.
pub struct Pki {
    pub cluster: Ca,
    pub other: Ca,
    pub foreign: Ca,
    /// The nodes' CA bundle: the cluster's and the other cluster's CAs.
    pub bundle: PathBuf,
}

impl Pki {
    pub fn new(dir: &Path) -> Self {
        let (cluster, other, foreign) = (Ca::new("cluster"), Ca::new("other"), Ca::new("foreign"));
        let bundle = dir.join("ca-bundle.pem");
        let text = pem("CERTIFICATE", &cluster.der) + &pem("CERTIFICATE", &other.der);
        std::fs::write(&bundle, text).unwrap();
        Self {
            cluster,
            other,
            foreign,
            bundle,
        }
    }
}

/// The `[control_store]` lines of the stand-in for etcd.
pub fn etcd(fake: &Fake) -> String {
    format!(
        "backend = \"etcd\"\netcd_endpoints = [\"{}\"]",
        fake.url("http")
    )
}

/// A node of the cluster in `dir`: node `node` over `store`, a shared
/// store, its transport on `port` with the certificate of `cert` and at
/// most `handshakes` handshakes at once.
pub fn node_config(
    dir: &Path,
    pki: &Pki,
    store: &str,
    node: &str,
    cert: &str,
    port: u16,
    handshakes: u32,
) -> String {
    let (cert, key) = pki.cluster.node(CLUSTER, cert).write(dir, "node");
    let transport = format!(
        "[transport]\nlisten = \"127.0.0.1:{port}\"\nmax_inflight_handshakes = {handshakes}\n\
         tls_cert_file = \"{}\"\ntls_key_file = \"{}\"\ntls_ca_file = \"{}\"",
        cert.display(),
        key.display(),
        pki.bundle.display()
    );
    config_text(dir, "127.0.0.1:0", "127.0.0.1:0", &transport)
        .replace(
            "[node]\n",
            &format!("[node]\nnode_id = \"{node}\"\nzone = \"z1\"\nrack = \"r-{node}\"\n"),
        )
        .replace("backend = \"file\"", store)
}
