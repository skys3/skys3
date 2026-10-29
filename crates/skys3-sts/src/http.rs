//! The production [`DocumentFetcher`]: HTTPS with rustls and a bounded body.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_LENGTH, USER_AGENT};
use http::{Request, StatusCode, Uri};
use http_body_util::{BodyExt, Empty, LengthLimitError, Limited};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use thiserror::Error;

use crate::fetch::{DocumentFetcher, FetchError};

/// How a [`HttpsFetcher`] connects.
#[derive(Clone, Debug)]
pub struct HttpsFetcherOptions {
    /// The limit on one request, from connecting to the last body byte.
    pub timeout: Duration,
    /// Whether to trust the operating system's root certificates.
    pub native_roots: bool,
    /// Additional trusted root certificates, such as a private CA that
    /// issues an in-cluster identity provider's certificate.
    pub extra_roots: Vec<CertificateDer<'static>>,
    /// Whether plain `http` URLs may be fetched. Off in production, where a
    /// network attacker could otherwise substitute an issuer's keys. Tests
    /// with a local identity provider turn it on.
    pub allow_http: bool,
}

impl Default for HttpsFetcherOptions {
    fn default() -> Self {
        HttpsFetcherOptions {
            timeout: Duration::from_secs(10),
            native_roots: true,
            extra_roots: Vec::new(),
            allow_http: false,
        }
    }
}

/// Why a [`HttpsFetcher`] could not be built.
#[derive(Debug, Error)]
pub enum HttpsFetcherError {
    /// No root certificate is trusted, so no HTTPS request could succeed.
    #[error("no trusted root certificates: the system store is empty or disabled")]
    NoRoots,
    /// The TLS configuration was rejected.
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
}

/// Fetches issuer documents over HTTPS.
///
/// Each request has an overall timeout, follows no redirects, needs status
/// 200, and reads at most the given number of body bytes. TLS uses rustls
/// with the `aws-lc-rs` provider, set per client rather than process-wide.
#[derive(Clone, Debug)]
pub struct HttpsFetcher {
    client: Client<HttpsConnector<HttpConnector>, Empty<Bytes>>,
    timeout: Duration,
    allow_http: bool,
}

impl HttpsFetcher {
    /// Builds a fetcher. The client, its connection pool, and its TLS
    /// configuration are shared by clones.
    ///
    /// # Errors
    ///
    /// Returns an error if no root certificate is trusted or rustls rejects
    /// the configuration.
    pub fn new(options: HttpsFetcherOptions) -> Result<Self, HttpsFetcherError> {
        let mut roots = RootCertStore::empty();
        if options.native_roots {
            let native = rustls_native_certs::load_native_certs();
            for error in &native.errors {
                tracing::warn!(%error, "could not load a system root certificate");
            }
            roots.add_parsable_certificates(native.certs);
        }
        for certificate in options.extra_roots {
            roots.add(certificate)?;
        }
        if roots.is_empty() {
            return Err(HttpsFetcherError::NoRoots);
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();

        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(options.timeout));
        let schemes = HttpsConnectorBuilder::new().with_tls_config(tls);
        let schemes = if options.allow_http {
            schemes.https_or_http()
        } else {
            schemes.https_only()
        };
        let connector = schemes.enable_http1().wrap_connector(http);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .build(connector);
        Ok(HttpsFetcher {
            client,
            timeout: options.timeout,
            allow_http: options.allow_http,
        })
    }

    fn check_url(&self, url: &str) -> Result<Uri, FetchError> {
        let rejected = |reason| FetchError::UrlRejected {
            url: url.to_owned(),
            reason,
        };
        let uri: Uri = url.parse().map_err(|_| rejected("not a URL"))?;
        match uri.scheme_str() {
            Some("https") => {}
            Some("http") if self.allow_http => {}
            Some("http") => return Err(rejected("plain http is not allowed")),
            _ => return Err(rejected("not an https URL")),
        }
        match uri.authority() {
            Some(authority) if authority.as_str().contains('@') => {
                Err(rejected("URL carries credentials"))
            }
            Some(_) => Ok(uri),
            None => Err(rejected("URL has no host")),
        }
    }

    async fn get(&self, uri: Uri, max_bytes: usize) -> Result<Bytes, FetchError> {
        let request = Request::get(uri)
            .header(ACCEPT, "application/json, application/jwk-set+json")
            .header(USER_AGENT, concat!("skys3/", env!("CARGO_PKG_VERSION")))
            .body(Empty::new())
            .map_err(|e| FetchError::Transport(e.to_string()))?;
        let response = self
            .client
            .request(request)
            .await
            .map_err(|e| FetchError::Transport(error_chain(&e)))?;
        if response.status() != StatusCode::OK {
            return Err(FetchError::Status(response.status().as_u16()));
        }
        let declared = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()?.parse::<u64>().ok());
        if declared.is_some_and(|length| length > max_bytes as u64) {
            return Err(FetchError::TooLarge { limit: max_bytes });
        }
        let body = Limited::new(response.into_body(), max_bytes)
            .collect()
            .await
            .map_err(|error| {
                if error.is::<LengthLimitError>() {
                    FetchError::TooLarge { limit: max_bytes }
                } else {
                    FetchError::Transport(error_chain(&*error))
                }
            })?;
        Ok(body.to_bytes())
    }
}

impl DocumentFetcher for HttpsFetcher {
    async fn fetch(&self, url: &str, max_bytes: usize) -> Result<Bytes, FetchError> {
        let uri = self.check_url(url)?;
        tokio::time::timeout(self.timeout, self.get(uri, max_bytes))
            .await
            .unwrap_or(Err(FetchError::Timeout))
    }
}

/// Formats an error with its sources, which carry the useful detail of
/// connection and TLS failures.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}
