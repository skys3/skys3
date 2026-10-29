//! The gateway's TLS configuration: `rustls` with the `aws-lc-rs`
//! provider, the one crypto stack of the binary (design §15).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Why the gateway's TLS configuration could not be built.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file could not be read or holds no usable item.
    #[error("{path}: {reason}")]
    File {
        /// The file.
        path: PathBuf,
        /// What is wrong.
        reason: String,
    },
    /// rustls refused the certificate chain or the key.
    #[error("the gateway's certificate or key was refused: {0}")]
    Rejected(#[from] rustls::Error),
}

/// Builds the server configuration from a PEM certificate chain, leaf
/// first, and a PEM private key. It offers TLS 1.3 and 1.2, and
/// HTTP/1.1 by ALPN.
///
/// # Errors
///
/// [`TlsError`] if a file cannot be read, holds no certificate or key, or
/// the key does not match the certificate.
pub fn server_config(cert_file: &Path, key_file: &Path) -> Result<Arc<ServerConfig>, TlsError> {
    let file_error = |path: &Path, reason: String| TlsError::File {
        path: path.to_owned(),
        reason,
    };
    let chain = CertificateDer::pem_file_iter(cert_file)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|error| file_error(cert_file, error.to_string()))?;
    if chain.is_empty() {
        return Err(file_error(cert_file, "holds no certificate".to_owned()));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|error| file_error(key_file, error.to_string()))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(chain, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data");

    #[test]
    fn loads_a_certificate_and_its_key() {
        let config = server_config(
            &Path::new(DATA).join("server.pem"),
            &Path::new(DATA).join("server.key"),
        )
        .unwrap();
        assert_eq!(config.alpn_protocols, [b"http/1.1".to_vec()]);
    }

    #[test]
    fn refuses_missing_and_mismatched_files() {
        let dir = tempfile::tempdir().unwrap();
        let cert = Path::new(DATA).join("server.pem");
        let key = Path::new(DATA).join("server.key");
        let missing = dir.path().join("missing.pem");
        let error = server_config(&missing, &key).unwrap_err();
        assert!(error.to_string().contains("missing.pem"), "{error}");
        let error = server_config(&cert, &missing).unwrap_err();
        assert!(matches!(error, TlsError::File { .. }));

        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, b"").unwrap();
        let error = server_config(&empty, &key).unwrap_err();
        assert!(error.to_string().contains("no certificate"), "{error}");

        // The CA's certificate does not match the server's key.
        let ca = Path::new(DATA).join("ca.pem");
        let error = server_config(&ca, &key).unwrap_err();
        assert!(matches!(error, TlsError::Rejected(_)), "{error}");
    }
}
