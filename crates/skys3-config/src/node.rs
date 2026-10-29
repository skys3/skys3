//! `[node]` and `[gateway]`: the node's identity, where it keeps its data,
//! and the S3 listener (§3, §10, §12).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use skys3_types::NodeId;

use crate::error::Checker;

/// `[node]`: the node's ID and its directories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    /// `node_id`: the node's ID. When unset, the node keeps the ID it
    /// generated when its data directory was created.
    pub node_id: Option<NodeId>,
    /// `data_dir`: the node's own files: its identity, the index, and the
    /// file control store unless `[control_store] directory` is set.
    pub data_dir: PathBuf,
    /// `disks`: one directory per disk, each holding that disk's log
    /// segments. Defaults to `<data_dir>/log`.
    pub disks: Vec<PathBuf>,
}

impl NodeConfig {
    /// The most disks a node uses.
    pub const MAX_DISKS: usize = 64;
}

/// `[node]` as written.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawNode {
    node_id: Option<String>,
    data_dir: PathBuf,
    disks: Option<Vec<PathBuf>>,
}

impl Default for RawNode {
    fn default() -> Self {
        Self {
            node_id: None,
            data_dir: PathBuf::from("/var/lib/skys3"),
            disks: None,
        }
    }
}

impl RawNode {
    /// Checks every key and resolves the section. An invalid node ID is
    /// reported and left unset.
    pub(crate) fn resolve(self, checker: &mut Checker) -> NodeConfig {
        let node_id = self.node_id.and_then(|id| match NodeId::new(id) {
            Ok(id) => Some(id),
            Err(error) => {
                checker.report("node.node_id", error);
                None
            }
        });
        nonempty_path(checker, "node.data_dir", &self.data_dir);
        let disks = match self.disks {
            Some(disks) => {
                checker.require(!disks.is_empty(), "node.disks", || {
                    "needs at least one directory; leave it out for <data_dir>/log".to_owned()
                });
                checker.require(disks.len() <= NodeConfig::MAX_DISKS, "node.disks", || {
                    format!(
                        "lists {} directories; a node uses at most {}",
                        disks.len(),
                        NodeConfig::MAX_DISKS
                    )
                });
                let mut seen = BTreeSet::new();
                for disk in &disks {
                    nonempty_path(checker, "node.disks", disk);
                    checker.require(seen.insert(disk), "node.disks", || {
                        format!("lists {} twice", disk.display())
                    });
                }
                disks
            }
            None => vec![self.data_dir.join("log")],
        };
        NodeConfig {
            node_id,
            data_dir: self.data_dir,
            disks,
        }
    }
}

/// `[gateway]`: the S3 and STS listener.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayListenConfig {
    /// `listen`: the TCP address the gateway serves S3 and STS on.
    pub listen: SocketAddr,
    /// `tls_cert_file`: a PEM file with the server's certificate chain,
    /// leaf first. With `tls_key_file`, the gateway serves HTTPS.
    pub tls_cert_file: Option<PathBuf>,
    /// `tls_key_file`: a PEM file with the server's private key.
    pub tls_key_file: Option<PathBuf>,
}

impl Default for GatewayListenConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 9000)),
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

impl GatewayListenConfig {
    /// Whether the gateway serves HTTPS.
    #[must_use]
    pub fn tls(&self) -> Option<(&Path, &Path)> {
        Some((
            self.tls_cert_file.as_deref()?,
            self.tls_key_file.as_deref()?,
        ))
    }

    pub(crate) fn check(&self, checker: &mut Checker) {
        for (key, value, other) in [
            (
                "gateway.tls_cert_file",
                &self.tls_cert_file,
                &self.tls_key_file,
            ),
            (
                "gateway.tls_key_file",
                &self.tls_key_file,
                &self.tls_cert_file,
            ),
        ] {
            match value {
                Some(path) => nonempty_path(checker, key, path),
                None => checker.require(other.is_none(), key, || {
                    "is required when the other TLS file is set".to_owned()
                }),
            }
        }
    }
}

/// Requires `path` not to be empty.
pub(crate) fn nonempty_path(checker: &mut Checker, key: &str, path: &Path) {
    checker.require(!path.as_os_str().is_empty(), key, || {
        "must not be empty".to_owned()
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_needs_both_files() {
        let mut checker = Checker::default();
        GatewayListenConfig {
            tls_cert_file: Some(PathBuf::from("/cert.pem")),
            ..GatewayListenConfig::default()
        }
        .check(&mut checker);
        let violations = checker.finish().unwrap_err();
        assert!(violations.contains_key("gateway.tls_key_file"));
        assert!(!violations.contains_key("gateway.tls_cert_file"));

        let both = GatewayListenConfig {
            tls_cert_file: Some(PathBuf::from("/cert.pem")),
            tls_key_file: Some(PathBuf::from("/key.pem")),
            ..GatewayListenConfig::default()
        };
        assert_eq!(
            both.tls(),
            Some((Path::new("/cert.pem"), Path::new("/key.pem")))
        );
        assert_eq!(GatewayListenConfig::default().tls(), None);
    }
}
