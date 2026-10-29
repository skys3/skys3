//! `[transport]`: the intra-cluster transport and the node's certificates
//! (§12).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::Checker;

/// `[transport]`: where the node accepts connections from other nodes, and
/// the files of its identity in the operator's PKI.
///
/// The three files are set together or not at all; without them the node
/// runs alone and never opens the transport.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// `listen`: the TCP address of the transport listener.
    pub listen: SocketAddr,
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

impl TransportConfig {
    /// The certificate files, or `None` if the node has none and runs
    /// alone.
    #[must_use]
    pub fn tls_files(&self) -> Option<TlsFiles<'_>> {
        Some(TlsFiles {
            cert: self.tls_cert_file.as_deref()?,
            key: self.tls_key_file.as_deref()?,
            ca: self.tls_ca_file.as_deref()?,
        })
    }

    pub(crate) fn check(&self, checker: &mut Checker) {
        let files = [
            ("transport.tls_cert_file", &self.tls_cert_file),
            ("transport.tls_key_file", &self.tls_key_file),
            ("transport.tls_ca_file", &self.tls_ca_file),
        ];
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
}
