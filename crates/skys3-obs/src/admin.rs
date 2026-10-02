//! The admin HTTP listener: health checks, metrics, and the admin API.
//!
//! # Endpoints
//!
//! | Path       | Methods     | Authentication                  |
//! |------------|-------------|---------------------------------|
//! | `/healthz` | GET, HEAD   | never                           |
//! | `/readyz`  | GET, HEAD   | never                           |
//! | `/metrics` | GET, HEAD   | bearer token, when one is set   |
//! | `/v1/...`  | the API's   | bearer token, when one is set   |
//! | any other  |             | bearer token, when one is set   |
//!
//! Paths under `/v1/` belong to the admin API, which the node supplies as
//! an [`AdminApi`] ([`AdminListener::with_api`]); without one they answer
//! 404.
//!
//! `/healthz` answers 200 while the listener serves. `/readyz` answers 200
//! when every [`Health`] component is ready, and 503 with the names of the
//! components that are not. Both stay unauthenticated so load balancers and
//! orchestrators can probe them without credentials; they disclose nothing
//! beyond component names.
//!
//! # Authentication
//!
//! Callers other than health checks authenticate with
//! `Authorization: Bearer <token>`, where the token is read from the file
//! that `[admin] token_file` names (design section 12). The listener binds
//! to loopback by default, where a token is optional. A non-loopback
//! address requires a token: [`AdminConfig::validate`] and
//! [`AdminListener::bind`] refuse one without it. When a token is set it
//! applies on every address, loopback included.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::health::Health;
use crate::metrics::{MetricsRegistry, OPENMETRICS_CONTENT_TYPE};

/// The default admin listen address: loopback only.
pub const DEFAULT_ADMIN_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7490);

/// The shortest accepted admin token, in bytes. `openssl rand -hex 32`
/// produces a 64-byte token.
pub const MIN_TOKEN_LEN: usize = 32;

/// How long a client may take to send its request headers.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How long shutdown waits for in-flight requests before dropping them.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// The most connections served at once. Further connections wait in the
/// kernel's accept queue, which bounds what unauthenticated callers of the
/// health endpoints can hold open.
const MAX_CONNECTIONS: usize = 256;

/// How long to pause after a failed `accept`, such as when the process has
/// run out of file descriptors.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Admin listener settings: the `[admin]` section of the configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminConfig {
    /// The address to listen on.
    pub listen: SocketAddr,
    /// The bearer token callers must present, loaded from `token_file`.
    /// Required when `listen` is not a loopback address.
    pub token: Option<AdminToken>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_ADMIN_LISTEN,
            token: None,
        }
    }
}

impl AdminConfig {
    /// Checks that a non-loopback listen address has a token.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::TokenRequired`] if `listen` is not a loopback
    /// address and no token is set.
    pub fn validate(&self) -> Result<(), AdminError> {
        if self.token.is_none() && !is_loopback(self.listen.ip()) {
            return Err(AdminError::TokenRequired {
                listen: self.listen,
            });
        }
        Ok(())
    }
}

/// Whether `ip` is a loopback address, including IPv4-mapped IPv6 forms
/// such as `::ffff:127.0.0.1`.
fn is_loopback(ip: IpAddr) -> bool {
    ip.to_canonical().is_loopback()
}

/// The admin bearer token.
///
/// It is compared in constant time and never printed: `Debug` shows only
/// that a token is present.
#[derive(Clone, PartialEq, Eq)]
pub struct AdminToken(Box<str>);

impl AdminToken {
    /// Validates a token: at least [`MIN_TOKEN_LEN`] bytes of visible ASCII
    /// with no spaces, so it fits an `Authorization` header unchanged.
    ///
    /// # Errors
    ///
    /// Returns an [`AdminTokenError`] naming the rule the token breaks.
    pub fn new(token: impl Into<String>) -> Result<Self, AdminTokenError> {
        let token = token.into();
        if !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(AdminTokenError::InvalidCharacter);
        }
        if token.len() < MIN_TOKEN_LEN {
            return Err(AdminTokenError::TooShort { len: token.len() });
        }
        Ok(Self(token.into_boxed_str()))
    }

    /// Reads a token from a file, ignoring trailing whitespace such as a
    /// final newline.
    ///
    /// # Errors
    ///
    /// Returns [`AdminTokenError::Read`] if the file cannot be read as
    /// UTF-8, and otherwise the errors of [`AdminToken::new`].
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, AdminTokenError> {
        let contents = std::fs::read_to_string(path).map_err(AdminTokenError::Read)?;
        Self::new(contents.trim_end())
    }

    /// Compares `candidate` with the token in time that depends only on
    /// their lengths, so a caller cannot find the token byte by byte.
    fn matches(&self, candidate: &[u8]) -> bool {
        let token = self.0.as_bytes();
        if token.len() != candidate.len() {
            return false;
        }
        token
            .iter()
            .zip(candidate)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
    }
}

impl fmt::Debug for AdminToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdminToken(<redacted>)")
    }
}

/// Why an admin token was rejected.
#[derive(Debug)]
pub enum AdminTokenError {
    /// The token file could not be read.
    Read(io::Error),
    /// The token contains a space, a control character, or non-ASCII.
    InvalidCharacter,
    /// The token is shorter than [`MIN_TOKEN_LEN`] bytes.
    TooShort {
        /// The token's length in bytes.
        len: usize,
    },
}

impl fmt::Display for AdminTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(err) => write!(f, "cannot read the admin token file: {err}"),
            Self::InvalidCharacter => {
                f.write_str("the admin token must be visible ASCII with no spaces")
            }
            Self::TooShort { len } => write!(
                f,
                "the admin token is {len} bytes, shorter than the minimum of {MIN_TOKEN_LEN}"
            ),
        }
    }
}

impl std::error::Error for AdminTokenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(err) => Some(err),
            Self::InvalidCharacter | Self::TooShort { .. } => None,
        }
    }
}

/// Why the admin listener could not start.
#[derive(Debug)]
pub enum AdminError {
    /// The listen address is not loopback and no token is set.
    TokenRequired {
        /// The configured listen address.
        listen: SocketAddr,
    },
    /// Binding the listen address failed.
    Bind {
        /// The configured listen address.
        listen: SocketAddr,
        /// The underlying error.
        source: io::Error,
    },
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TokenRequired { listen } => write!(
                f,
                "admin listener on non-loopback address {listen} requires [admin] token_file"
            ),
            Self::Bind { listen, source } => {
                write!(f, "cannot bind the admin listener to {listen}: {source}")
            }
        }
    }
}

impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TokenRequired { .. } => None,
            Self::Bind { source, .. } => Some(source),
        }
    }
}

/// An admin listener's answer.
pub type AdminResponse = Response<Full<Bytes>>;

/// The future of [`AdminApi::call`]: the answer, or `None` for a path the
/// API does not have.
pub type ApiFuture<'a> = Pin<Box<dyn Future<Output = Option<AdminResponse>> + Send + 'a>>;

/// The admin API: the routes under `/v1/`, which the node serves through
/// the admin listener, behind its authentication (design section 12).
pub trait AdminApi: Send + Sync + 'static {
    /// Answers `method` on `path`, which starts with `/v1/`, or returns
    /// `None` for a path the API does not have, which the listener answers
    /// with 404. The caller is already authenticated.
    fn call<'a>(&'a self, method: &'a Method, path: &'a str) -> ApiFuture<'a>;
}

/// The labels of `skys3_admin_requests_total`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, EncodeLabelSet)]
struct RequestLabels {
    endpoint: &'static str,
    code: u16,
}

/// The routes the listener knows. Anything else is [`Endpoint::Other`], so
/// request metrics have a bounded set of label values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    Healthz,
    Readyz,
    Metrics,
    Api,
    Other,
}

impl Endpoint {
    /// The prefix of every admin API path.
    const API_PREFIX: &'static str = "/v1/";

    fn from_path(path: &str) -> Self {
        match path {
            "/healthz" => Self::Healthz,
            "/readyz" => Self::Readyz,
            "/metrics" => Self::Metrics,
            _ if path.starts_with(Self::API_PREFIX) => Self::Api,
            _ => Self::Other,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Healthz => "healthz",
            Self::Readyz => "readyz",
            Self::Metrics => "metrics",
            Self::Api => "api",
            Self::Other => "other",
        }
    }

    /// Health checks stay open to load balancers and orchestrators.
    const fn requires_auth(self) -> bool {
        !matches!(self, Self::Healthz | Self::Readyz)
    }
}

/// What every connection's request handler shares.
struct AdminState {
    token: Option<AdminToken>,
    metrics: MetricsRegistry,
    health: Health,
    requests: Family<RequestLabels, Counter>,
    api: Option<Arc<dyn AdminApi>>,
}

impl AdminState {
    /// Answers one request, the admin API's included, and counts it.
    async fn serve<B>(&self, request: &Request<B>) -> Response<Full<Bytes>> {
        let path = request.uri().path();
        let endpoint = Endpoint::from_path(path);
        if endpoint != Endpoint::Api {
            return self.respond(request);
        }
        let response = if !authorized(self.token.as_ref(), request.headers()) {
            unauthorized()
        } else if let Some(api) = &self.api {
            api.call(request.method(), path)
                .await
                .unwrap_or_else(|| text(StatusCode::NOT_FOUND, "not found\n"))
        } else {
            text(StatusCode::NOT_FOUND, "not found\n")
        };
        self.count(endpoint, request, &response);
        response
    }

    /// Answers one request outside the admin API and counts it.
    fn respond<B>(&self, request: &Request<B>) -> Response<Full<Bytes>> {
        let endpoint = Endpoint::from_path(request.uri().path());
        let response = self.route(endpoint, request.method(), request.headers());
        self.count(endpoint, request, &response);
        response
    }

    fn count<B>(&self, endpoint: Endpoint, request: &Request<B>, response: &Response<Full<Bytes>>) {
        self.requests
            .get_or_create(&RequestLabels {
                endpoint: endpoint.label(),
                code: response.status().as_u16(),
            })
            .inc();
        tracing::debug!(
            method = %request.method(),
            path = request.uri().path(),
            status = response.status().as_u16(),
            "admin request"
        );
    }

    fn route(
        &self,
        endpoint: Endpoint,
        method: &Method,
        headers: &HeaderMap,
    ) -> Response<Full<Bytes>> {
        if endpoint.requires_auth() && !authorized(self.token.as_ref(), headers) {
            return unauthorized();
        }
        let handler: fn(&Self) -> Response<Full<Bytes>> = match endpoint {
            Endpoint::Healthz => |_| text(StatusCode::OK, "ok\n"),
            Endpoint::Readyz => Self::readiness,
            Endpoint::Metrics => Self::scrape,
            Endpoint::Api | Endpoint::Other => {
                return text(StatusCode::NOT_FOUND, "not found\n");
            }
        };
        if method != Method::GET && method != Method::HEAD {
            let mut response = text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return response;
        }
        handler(self)
    }

    fn readiness(&self) -> Response<Full<Bytes>> {
        let not_ready = self.health.not_ready();
        if not_ready.is_empty() {
            text(StatusCode::OK, "ready\n")
        } else {
            let body = format!("not ready: {}\n", not_ready.join(", "));
            text(StatusCode::SERVICE_UNAVAILABLE, body)
        }
    }

    fn scrape(&self) -> Response<Full<Bytes>> {
        match self.metrics.encode() {
            Ok(body) => with_content_type(StatusCode::OK, OPENMETRICS_CONTENT_TYPE, body),
            Err(err) => {
                tracing::error!(error = %err, "cannot encode metrics");
                text(StatusCode::INTERNAL_SERVER_ERROR, "cannot encode metrics\n")
            }
        }
    }
}

/// Whether `headers` carry `token` as bearer credentials. Every request is
/// authorized when no token is set.
pub(crate) fn authorized(token: Option<&AdminToken>, headers: &HeaderMap) -> bool {
    let Some(token) = token else {
        return true;
    };
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| bearer_credentials(value.as_bytes()))
        .is_some_and(|candidate| token.matches(candidate))
}

/// Returns the credentials of an `Authorization: Bearer` header value, with
/// surrounding ASCII whitespace removed, or `None` if the value uses
/// another scheme or carries no credentials. The scheme name is
/// case-insensitive and one space separates it from the credentials
/// (RFC 9110, section 11.4).
pub(crate) fn bearer_credentials(value: &[u8]) -> Option<&[u8]> {
    const SCHEME: &[u8] = b"bearer ";
    let (scheme, credentials) = value.split_at_checked(SCHEME.len())?;
    if !scheme.eq_ignore_ascii_case(SCHEME) {
        return None;
    }
    Some(credentials.trim_ascii()).filter(|credentials| !credentials.is_empty())
}

/// The answer to a request without the bearer token.
fn unauthorized() -> Response<Full<Bytes>> {
    let mut response = text(StatusCode::UNAUTHORIZED, "unauthorized\n");
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Bearer realm="skys3-admin""#),
    );
    response
}

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    with_content_type(status, "text/plain; charset=utf-8", body)
}

fn with_content_type(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// A bound admin listener, ready to serve.
pub struct AdminListener {
    listener: TcpListener,
    state: Arc<AdminState>,
    drain_timeout: Duration,
}

impl AdminListener {
    /// Validates `config` and binds its listen address.
    ///
    /// Registers `skys3_admin_requests_total` in `metrics`, so each registry
    /// serves at most one admin listener.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::TokenRequired`] if the address is not loopback
    /// and no token is set, and [`AdminError::Bind`] if binding fails.
    pub async fn bind(
        config: AdminConfig,
        metrics: MetricsRegistry,
        health: Health,
    ) -> Result<Self, AdminError> {
        config.validate()?;
        let listener =
            TcpListener::bind(config.listen)
                .await
                .map_err(|source| AdminError::Bind {
                    listen: config.listen,
                    source,
                })?;
        let requests = Family::<RequestLabels, Counter>::default();
        metrics.register(
            "admin_requests",
            "Requests answered by the admin HTTP listener, by endpoint and status code.",
            requests.clone(),
        );
        Ok(Self {
            listener,
            state: Arc::new(AdminState {
                token: config.token,
                metrics,
                health,
                requests,
                api: None,
            }),
            drain_timeout: SHUTDOWN_DRAIN_TIMEOUT,
        })
    }

    /// Serves `api` under `/v1/`, behind the listener's authentication.
    #[must_use]
    pub fn with_api(mut self, api: Arc<dyn AdminApi>) -> Self {
        let state = Arc::get_mut(&mut self.state).expect("the state is shared only once serving");
        state.api = Some(api);
        self
    }

    /// Returns the bound address, which tells the port when the configured
    /// port is 0.
    ///
    /// # Errors
    ///
    /// Returns the operating system's error if the address is unavailable.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves connections until `shutdown` completes, then stops accepting
    /// and gives in-flight requests the drain deadline (5 seconds) to
    /// finish. Connections still open at the deadline are dropped, so when
    /// `serve` returns no connection of this listener remains.
    ///
    /// A failed `accept` is logged and retried after a pause, so running
    /// out of file descriptors does not stop the listener for good.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) {
        let Self {
            listener,
            state,
            drain_timeout,
        } = self;
        let graceful = GracefulShutdown::new();
        let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let mut connections = JoinSet::new();
        let mut builder = http1::Builder::new();
        builder
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_READ_TIMEOUT);
        tokio::pin!(shutdown);

        loop {
            // Reap finished connections. At most one more connection is
            // accepted per pass, so the set stays within MAX_CONNECTIONS
            // plus one entry.
            while connections.try_join_next().is_some() {}
            let slot = tokio::select! {
                () = &mut shutdown => break,
                slot = Arc::clone(&slots).acquire_owned() => {
                    slot.expect("the connection semaphore is never closed")
                }
            };
            let (stream, peer) = tokio::select! {
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        tracing::warn!(error = %err, "admin listener cannot accept a connection");
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                },
            };
            let state = Arc::clone(&state);
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let state = Arc::clone(&state);
                async move { Ok::<_, Infallible>(state.serve(&request).await) }
            });
            let connection =
                graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
            connections.spawn(async move {
                // The slot is released when the task ends or is aborted.
                let _slot = slot;
                if let Err(err) = connection.await {
                    tracing::debug!(%peer, error = %err, "admin connection ended with an error");
                }
            });
        }

        drop(listener);
        tokio::select! {
            () = graceful.shutdown() => {}
            () = tokio::time::sleep(drain_timeout) => {
                tracing::warn!(
                    connections = connections.len(),
                    "admin listener dropped connections still open at the drain deadline"
                );
            }
        }
        // Abort what the deadline cut off, and wait until every task,
        // with its socket, slot, and share of the state, is gone.
        connections.shutdown().await;
    }
}

impl fmt::Debug for AdminListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdminListener")
            .field("local_addr", &self.listener.local_addr().ok())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use http_body_util::BodyExt;

    use super::*;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn state(token: Option<&str>) -> AdminState {
        AdminState {
            token: token.map(|t| AdminToken::new(t).unwrap()),
            metrics: MetricsRegistry::new(),
            health: Health::new(),
            requests: Family::default(),
            api: None,
        }
    }

    fn request(method: Method, path: &str, authorization: Option<&str>) -> Request<()> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(value) = authorization {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        builder.body(()).unwrap()
    }

    fn status(
        state: &AdminState,
        method: Method,
        path: &str,
        authorization: Option<&str>,
    ) -> StatusCode {
        state
            .respond(&request(method, path, authorization))
            .status()
    }

    #[test]
    fn loopback_detection_covers_every_form() {
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(127, 1, 2, 3)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ] {
            assert!(is_loopback(ip), "{ip}");
        }
        for ip in [
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        ] {
            assert!(!is_loopback(ip), "{ip}");
        }
    }

    #[test]
    fn non_loopback_listen_requires_a_token() {
        assert!(AdminConfig::default().validate().is_ok());
        let exposed = AdminConfig {
            listen: "0.0.0.0:7490".parse().unwrap(),
            token: None,
        };
        let err = exposed.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "admin listener on non-loopback address 0.0.0.0:7490 requires [admin] token_file"
        );
        assert!(std::error::Error::source(&err).is_none());
        let with_token = AdminConfig {
            token: Some(AdminToken::new(TOKEN).unwrap()),
            ..exposed
        };
        assert!(with_token.validate().is_ok());
    }

    #[test]
    fn token_rules() {
        assert!(AdminToken::new(TOKEN).is_ok());
        assert!(matches!(
            AdminToken::new(&TOKEN[1..]),
            Err(AdminTokenError::TooShort { len: 31 })
        ));
        for bad in [
            format!("{TOKEN} x"),
            format!("{TOKEN}\n"),
            format!("{TOKEN}é"),
        ] {
            assert!(matches!(
                AdminToken::new(bad),
                Err(AdminTokenError::InvalidCharacter)
            ));
        }
        assert_eq!(
            format!("{:?}", AdminToken::new(TOKEN).unwrap()),
            "AdminToken(<redacted>)"
        );
        let short = AdminToken::new("x").unwrap_err();
        assert_eq!(
            short.to_string(),
            "the admin token is 1 bytes, shorter than the minimum of 32"
        );
        assert!(std::error::Error::source(&short).is_none());
        let invalid = AdminToken::new(" ").unwrap_err();
        assert_eq!(
            invalid.to_string(),
            "the admin token must be visible ASCII with no spaces"
        );
    }

    #[test]
    fn token_from_file() {
        let dir = std::env::temp_dir().join(format!("skys3-obs-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("admin.token");
        std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
        assert_eq!(
            AdminToken::from_file(&path).unwrap(),
            AdminToken::new(TOKEN).unwrap()
        );

        let missing = AdminToken::from_file(dir.join("missing")).unwrap_err();
        assert!(matches!(missing, AdminTokenError::Read(_)));
        assert!(
            missing
                .to_string()
                .starts_with("cannot read the admin token file")
        );
        assert!(std::error::Error::source(&missing).is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn token_comparison() {
        let token = AdminToken::new(TOKEN).unwrap();
        assert!(token.matches(TOKEN.as_bytes()));
        assert!(!token.matches(b""));
        assert!(!token.matches(&TOKEN.as_bytes()[1..]));
        let mut flipped = TOKEN.as_bytes().to_vec();
        flipped[31] ^= 1;
        assert!(!token.matches(&flipped));
    }

    #[test]
    fn bearer_header_parsing() {
        assert_eq!(bearer_credentials(b"Bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_credentials(b"bearer   abc "), Some(&b"abc"[..]));
        assert_eq!(bearer_credentials(b"BEARER abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_credentials(b"Basic abc"), None);
        assert_eq!(bearer_credentials(b"Bearer"), None);
        assert_eq!(bearer_credentials(b"Bearer "), None);
        assert_eq!(bearer_credentials(b"Bearer \t "), None);
        assert_eq!(bearer_credentials(b"Bearer\tabc"), None);
    }

    #[test]
    fn health_endpoints_never_need_a_token() {
        let state = state(Some(TOKEN));
        assert_eq!(
            status(&state, Method::GET, "/healthz", None),
            StatusCode::OK
        );
        assert_eq!(
            status(&state, Method::HEAD, "/healthz", None),
            StatusCode::OK
        );
        assert_eq!(status(&state, Method::GET, "/readyz", None), StatusCode::OK);
    }

    #[test]
    fn other_endpoints_need_the_token_when_one_is_set() {
        let state = state(Some(TOKEN));
        let good = format!("Bearer {TOKEN}");
        let response = state.respond(&request(Method::GET, "/metrics", None));
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::WWW_AUTHENTICATE],
            r#"Bearer realm="skys3-admin""#
        );
        let wrong = format!("Bearer {}", TOKEN.replace('0', "1"));
        assert_eq!(
            status(&state, Method::GET, "/metrics", Some(&wrong)),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&state, Method::GET, "/metrics", Some(TOKEN)),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&state, Method::GET, "/metrics", Some(&good)),
            StatusCode::OK
        );
        // Unknown paths are not revealed to unauthenticated callers.
        assert_eq!(
            status(&state, Method::GET, "/admin", None),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&state, Method::GET, "/admin", Some(&good)),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn no_token_means_open_access() {
        let state = state(None);
        assert_eq!(
            status(&state, Method::GET, "/metrics", None),
            StatusCode::OK
        );
        assert_eq!(
            status(&state, Method::GET, "/nope", None),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn readiness_reports_components_that_are_not_ready() {
        let state = state(None);
        let recovery = state.health.register("recovery");
        let response = state.respond(&request(Method::GET, "/readyz", None));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, "not ready: recovery\n");
        recovery.set_ready(true);
        assert_eq!(status(&state, Method::GET, "/readyz", None), StatusCode::OK);
    }

    #[test]
    fn only_get_and_head_are_allowed() {
        let state = state(None);
        let response = state.respond(&request(Method::POST, "/metrics", None));
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[header::ALLOW], "GET, HEAD");
        assert_eq!(
            status(&state, Method::DELETE, "/healthz", None),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    #[test]
    fn metrics_response_headers() {
        let response = state(None).respond(&request(Method::GET, "/metrics", None));
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            OPENMETRICS_CONTENT_TYPE
        );
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[test]
    fn requests_are_counted_by_endpoint_and_code() {
        let state = state(Some(TOKEN));
        state
            .metrics
            .register("admin_requests", "Requests.", state.requests.clone());
        let _ = status(&state, Method::GET, "/metrics", None);
        let _ = status(&state, Method::GET, "/some/unbounded/path", None);
        let _ = status(&state, Method::GET, "/healthz", None);
        let text = state.metrics.encode().unwrap();
        for line in [
            r#"skys3_admin_requests_total{endpoint="metrics",code="401"} 1"#,
            r#"skys3_admin_requests_total{endpoint="other",code="401"} 1"#,
            r#"skys3_admin_requests_total{endpoint="healthz",code="200"} 1"#,
        ] {
            assert!(text.contains(line), "missing {line} in {text}");
        }
    }

    /// An API that knows one path.
    struct OnePath;

    impl AdminApi for OnePath {
        fn call<'a>(&'a self, method: &'a Method, path: &'a str) -> ApiFuture<'a> {
            Box::pin(async move {
                (path == "/v1/known").then(|| text(StatusCode::OK, method.to_string()))
            })
        }
    }

    #[tokio::test]
    async fn api_paths_go_to_the_api_behind_the_token() {
        let mut state = state(Some(TOKEN));
        state
            .metrics
            .register("admin_requests", "Requests.", state.requests.clone());
        let bearer = format!("Bearer {TOKEN}");
        let missing = state
            .serve(&request(Method::GET, "/v1/known", Some(&bearer)))
            .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND, "no API yet");
        state.api = Some(Arc::new(OnePath));
        let denied = state.serve(&request(Method::GET, "/v1/known", None)).await;
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert!(denied.headers().contains_key(header::WWW_AUTHENTICATE));
        let known = state
            .serve(&request(Method::POST, "/v1/known", Some(&bearer)))
            .await;
        assert_eq!(known.status(), StatusCode::OK);
        let unknown = state
            .serve(&request(Method::GET, "/v1/unknown", Some(&bearer)))
            .await;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let health = state.serve(&request(Method::GET, "/healthz", None)).await;
        assert_eq!(health.status(), StatusCode::OK);
        let text = state.metrics.encode().unwrap();
        assert!(
            text.contains(r#"skys3_admin_requests_total{endpoint="api",code="200"} 1"#),
            "{text}"
        );
    }

    /// A client that stops mid-request must not keep its connection, and
    /// with it the listener's state and a connection slot, alive after
    /// `serve` returns.
    #[tokio::test]
    async fn stuck_connection_is_dropped_after_the_drain_deadline() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let config = AdminConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            token: None,
        };
        let mut listener = AdminListener::bind(config, MetricsRegistry::new(), Health::new())
            .await
            .unwrap();
        listener.drain_timeout = Duration::from_millis(200);
        let addr = listener.local_addr().unwrap();
        let state = Arc::clone(&listener.state);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let serving = tokio::spawn(listener.serve(async {
            let _ = stopped.await;
        }));

        // Headers without their final blank line: the request never ends.
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: skys3\r\n")
            .await
            .unwrap();
        // Wait until the server has accepted the connection: the listener,
        // the connection's service, and this test then hold the state.
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&state) < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the server accepts the connection");
        // Let hyper read the partial head, so the connection is mid-request.
        tokio::time::sleep(Duration::from_millis(50)).await;

        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("serve returns after the drain deadline")
            .unwrap();
        assert_eq!(
            Arc::strong_count(&state),
            1,
            "the connection task was dropped"
        );
        let mut buf = [0; 64];
        let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf))
            .await
            .expect("the server closed the connection");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
    }

    #[test]
    fn bind_error_messages() {
        let err = AdminError::Bind {
            listen: DEFAULT_ADMIN_LISTEN,
            source: io::Error::from(io::ErrorKind::AddrInUse),
        };
        assert!(
            err.to_string()
                .starts_with("cannot bind the admin listener to 127.0.0.1:7490")
        );
        assert!(std::error::Error::source(&err).is_some());
    }

    mod properties {
        use proptest::prelude::*;

        use super::*;

        /// Header whitespace: space and horizontal tab.
        fn whitespace() -> impl Strategy<Value = String> {
            proptest::string::string_regex("[ \t]{0,4}").unwrap()
        }

        /// "Bearer" in an arbitrary mix of upper and lower case.
        fn scheme() -> impl Strategy<Value = String> {
            proptest::collection::vec(any::<bool>(), 6).prop_map(|upper| {
                "bearer"
                    .chars()
                    .zip(upper)
                    .map(|(c, up)| if up { c.to_ascii_uppercase() } else { c })
                    .collect()
            })
        }

        proptest! {
            #[test]
            fn parsing_arbitrary_bytes_keeps_its_invariants(value in any::<Vec<u8>>()) {
                if let Some(credentials) = bearer_credentials(&value) {
                    prop_assert!(value[..7].eq_ignore_ascii_case(b"bearer "));
                    prop_assert!(!credentials.is_empty());
                    prop_assert_eq!(credentials, credentials.trim_ascii());
                    // The credentials are a slice of the value, after the scheme.
                    let offset = credentials.as_ptr() as usize - value.as_ptr() as usize;
                    prop_assert!(offset >= 7 && offset + credentials.len() <= value.len());
                }
            }

            #[test]
            fn well_formed_values_round_trip(
                scheme in scheme(),
                leading in whitespace(),
                token in "[!-~]{1,128}",
                trailing in whitespace(),
            ) {
                let value = format!("{scheme} {leading}{token}{trailing}");
                prop_assert_eq!(bearer_credentials(value.as_bytes()), Some(token.as_bytes()));
            }

            #[test]
            fn only_the_exact_token_is_authorized(
                scheme in scheme(),
                candidate in "[!-~]{0,40}",
            ) {
                let token = AdminToken::new(TOKEN).unwrap();
                let mut headers = HeaderMap::new();
                let value = format!("{scheme} {candidate}");
                headers.insert(header::AUTHORIZATION, HeaderValue::from_str(&value).unwrap());
                prop_assert_eq!(authorized(Some(&token), &headers), candidate == TOKEN);
                headers.insert(
                    header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("{scheme} {TOKEN}")).unwrap(),
                );
                prop_assert!(authorized(Some(&token), &headers));
                prop_assert!(authorized(None, &HeaderMap::new()));
            }
        }
    }
}
