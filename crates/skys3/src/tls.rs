//! The node's TLS configurations: the gateway's, and the etcd control
//! store's client (design §12). `rustls` with the `aws-lc-rs` provider,
//! the one crypto stack of the binary (design §15).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// Why a TLS configuration could not be built.
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
    /// No CA file was named, and no system root certificate could be
    /// loaded.
    #[error("no system root certificate could be loaded; name a CA file")]
    NoRoots,
    /// rustls refused a certificate chain or key.
    #[error("a certificate or key was refused: {0}")]
    Rejected(#[from] rustls::Error),
}

/// The certificates of a PEM file, at least one.
fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file_error = |reason: String| TlsError::File {
        path: path.to_owned(),
        reason,
    };
    let certificates = CertificateDer::pem_file_iter(path)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|error| file_error(error.to_string()))?;
    if certificates.is_empty() {
        return Err(file_error("holds no certificate".to_owned()));
    }
    Ok(certificates)
}

/// The private key of a PEM file.
fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    PrivateKeyDer::from_pem_file(path).map_err(|error| TlsError::File {
        path: path.to_owned(),
        reason: error.to_string(),
    })
}

/// Builds a client configuration that verifies servers against the CA
/// certificates of `ca_file`, or the system's root certificates without
/// one, and presents `client`'s PEM certificate chain and key, if given.
/// It offers TLS 1.3 and 1.2; the caller sets ALPN.
///
/// # Errors
///
/// [`TlsError`] if a file cannot be read or holds nothing usable, no root
/// certificate is found, or the key does not match the certificate.
pub fn client_config(
    ca_file: Option<&Path>,
    client: Option<(&Path, &Path)>,
) -> Result<Arc<ClientConfig>, TlsError> {
    let mut roots = RootCertStore::empty();
    match ca_file {
        Some(path) => {
            for certificate in certificates(path)? {
                roots.add(certificate)?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for error in &native.errors {
                tracing::warn!(%error, "could not load a system root certificate");
            }
            roots.add_parsable_certificates(native.certs);
            if roots.is_empty() {
                return Err(TlsError::NoRoots);
            }
        }
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots);
    let config = match client {
        Some((cert_file, key_file)) => {
            builder.with_client_auth_cert(certificates(cert_file)?, private_key(key_file)?)?
        }
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
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
    let chain = certificates(cert_file)?;
    let key = private_key(key_file)?;
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

    #[test]
    fn client_configurations_load_a_ca_and_a_client_certificate() {
        let data = Path::new(DATA);
        let (ca, cert, key) = (
            data.join("ca.pem"),
            data.join("server.pem"),
            data.join("server.key"),
        );
        client_config(Some(&ca), None).unwrap();
        client_config(Some(&ca), Some((&cert, &key))).unwrap();
        let error = client_config(Some(&key), None).unwrap_err();
        assert!(error.to_string().contains("no certificate"), "{error}");
        let error = client_config(Some(&ca), Some((&ca, &key))).unwrap_err();
        assert!(matches!(error, TlsError::Rejected(_)), "{error}");
        let error = client_config(Some(&ca), Some((&cert, &cert))).unwrap_err();
        assert!(matches!(error, TlsError::File { .. }), "{error}");
        // The system's roots, if this machine has any.
        match client_config(None, None) {
            Ok(_) | Err(TlsError::NoRoots) => {}
            Err(error) => panic!("{error}"),
        }
    }
}
