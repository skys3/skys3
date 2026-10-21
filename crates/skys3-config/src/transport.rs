//! `[transport]`: the intra-cluster transport and the node's certificates
//! (§6.7, §12).

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use skys3_types::{Host, NodeAddress};

use crate::error::Checker;

/// The default cap on the TLS handshakes a listener runs at once:
/// `[transport] max_inflight_handshakes`, `[gateway]
/// max_inflight_handshakes`, and `[peering] peer_max_inflight_handshakes`
/// (§12). The admin listener's fixed cap on its connections is the same.
pub const DEFAULT_MAX_INFLIGHT_HANDSHAKES: u32 = 256;

/// The largest cap on in-flight handshakes a listener accepts.
pub const MAX_INFLIGHT_HANDSHAKES: u32 = 65_536;

/// Requires a cap on in-flight handshakes to be from 1 to
/// [`MAX_INFLIGHT_HANDSHAKES`].
pub(crate) fn check_handshake_cap(checker: &mut Checker, key: &str, cap: u32) {
    checker.require((1..=MAX_INFLIGHT_HANDSHAKES).contains(&cap), key, || {
        format!("must be from 1 to {MAX_INFLIGHT_HANDSHAKES}, not {cap}")
    });
}

/// `[transport]`: where the node accepts connections from other nodes, and
/// the files of its identity in the operator's PKI.
///
/// The three files are set together or not at all. A node of a cluster,
/// over an etcd or S3 control store, binds the listener and requires them;
/// a single node, over the file store, binds none. Peering
/// (`[peering.peers]`) presents the same certificate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// `listen`: the TCP address of the transport listener.
    pub listen: SocketAddr,
    /// `advertise`: the `host:port` other nodes reach the listener at,
    /// which the node registers in `nodes/<node-id>.json` (§6.7). Unset:
    /// `listen`, unless it is a wildcard address.
    pub advertise: Option<NodeAddress>,
    /// `max_inflight_handshakes`: how many TLS handshakes the listener
    /// runs at once (§12). Further connections wait in the kernel's
    /// accept queue until one finishes.
    pub max_inflight_handshakes: u32,
    /// `tls_cert_file`: the node's certificate chain in PEM, leaf first.
    /// The leaf names the node with its SPIFFE ID.
    pub tls_cert_file: Option<PathBuf>,
    /// `tls_key_file`: the private key of the node certificate, in PEM.
    pub tls_key_file: Option<PathBuf>,
    /// `tls_ca_file`: the CA certificates peers' chains must lead to, in
    /// PEM.
    pub tls_ca_file: Option<PathBuf>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7400)),
            advertise: None,
            max_inflight_handshakes: DEFAULT_MAX_INFLIGHT_HANDSHAKES,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_file: None,
        }
    }
}

/// The certificate files of a node, all present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlsFiles<'a> {
    /// `tls_cert_file`.
    pub cert: &'a Path,
    /// `tls_key_file`.
    pub key: &'a Path,
    /// `tls_ca_file`.
    pub ca: &'a Path,
}

/// The keys of the certificate files, with their values.
type FileKeys<'a> = [(&'static str, &'a Option<PathBuf>); 3];

impl TransportConfig {
    /// The certificate files, or `None` if the node has none.
    #[must_use]
    pub fn tls_files(&self) -> Option<TlsFiles<'_>> {
        Some(TlsFiles {
            cert: self.tls_cert_file.as_deref()?,
            key: self.tls_key_file.as_deref()?,
            ca: self.tls_ca_file.as_deref()?,
        })
    }

    /// The address the node registers for other nodes to reach its
    /// listener at: `advertise`, or else `listen` if it is neither a
    /// wildcard address nor port 0. `None` otherwise, which a node of a
    /// cluster does not allow.
    #[must_use]
    pub fn advertised(&self) -> Option<NodeAddress> {
        if let Some(advertise) = &self.advertise {
            return Some(advertise.clone());
        }
        let port = NonZeroU16::new(self.listen.port())?;
        let host = match self.listen.ip() {
            ip if ip.is_unspecified() => return None,
            IpAddr::V4(ip) => Host::Ipv4(ip),
            IpAddr::V6(ip) => Host::Ipv6(ip),
        };
        Some(NodeAddress::new(host, port))
    }

    fn file_keys(&self) -> FileKeys<'_> {
        [
            ("transport.tls_cert_file", &self.tls_cert_file),
            ("transport.tls_key_file", &self.tls_key_file),
            ("transport.tls_ca_file", &self.tls_ca_file),
        ]
    }

    pub(crate) fn check(&self, checker: &mut Checker) {
        check_handshake_cap(
            checker,
            "transport.max_inflight_handshakes",
            self.max_inflight_handshakes,
        );
        let files = self.file_keys();
        let set = files.iter().filter(|(_, file)| file.is_some()).count();
        for (key, file) in files {
            match file {
                Some(path) => checker.require(!path.as_os_str().is_empty(), key, || {
                    "must not be empty".to_owned()
                }),
                None => checker.require(set == 0, key, || {
                    "is required when another tls_*_file key is set; set all three or none"
                        .to_owned()
                }),
            }
        }
    }

    /// The rules of a node of a cluster, over an etcd or S3 control store:
    /// the node's certificates, and an address to register.
    pub(crate) fn check_cluster_node(&self, checker: &mut Checker) {
        let files = self.file_keys();
        if files.iter().all(|(_, file)| file.is_none()) {
            for (key, _) in files {
                checker.report(
                    key,
                    "is required with an etcd or S3 control store, whose nodes form a cluster \
                     and authenticate each other with their certificates; a single node uses \
                     backend = \"file\"",
                );
            }
        }
        if self.advertised().is_none() {
            checker.report(
                "transport.advertise",
                format!(
                    "is required with an etcd or S3 control store while listen ({}) is a \
                     wildcard address or port 0: the cluster's other nodes must know where to \
                     reach this node",
                    self.listen
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transport(listen: &str) -> TransportConfig {
        TransportConfig {
            listen: listen.parse().unwrap(),
            ..TransportConfig::default()
        }
    }

    #[test]
    fn the_advertised_address_defaults_to_a_specific_listen_address() {
        assert_eq!(transport("0.0.0.0:7400").advertised(), None);
        assert_eq!(transport("[::]:7400").advertised(), None);
        assert_eq!(transport("10.0.0.5:0").advertised(), None);
        assert_eq!(
            transport("10.0.0.5:7400").advertised(),
            Some("10.0.0.5:7400".parse().unwrap())
        );
        assert_eq!(
            transport("[fd00::5]:7400").advertised(),
            Some("[fd00::5]:7400".parse().unwrap())
        );
        let advertised = TransportConfig {
            advertise: Some("node-1.example.internal:7400".parse().unwrap()),
            ..transport("0.0.0.0:7400")
        };
        assert_eq!(
            advertised.advertised(),
            Some("node-1.example.internal:7400".parse().unwrap())
        );
    }
}
