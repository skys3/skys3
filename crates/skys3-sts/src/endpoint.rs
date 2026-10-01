//! The STS endpoint: `AssumeRoleWithWebIdentity` over the STS query API
//! (design §11).
//!
//! The gateway serves STS on its own listener: a `POST` to `/` is an STS
//! request ([`skys3_gateway::StsService`]), so clients point
//! `AWS_ENDPOINT_URL_STS` at the same URL as `AWS_ENDPOINT_URL_S3`.
//! [`StsEndpoint`] answers it:
//!
//! 1. The body must be form-encoded (`application/x-www-form-urlencoded`)
//!    and at most [`MAX_FORM_BYTES`]; the parameters are parsed strictly
//!    ([`request`]).
//! 2. `DurationSeconds`, default `session_default_seconds`, must be from
//!    900 to `session_maximum_seconds`. A `Policy` must parse in the policy
//!    subset (`MalformedPolicyDocument`).
//! 3. The node's identity copy must be fresh: once it is older than
//!    `identity_max_staleness`, no session is issued (`503
//!    ServiceUnavailable`) until it syncs again ([`crate::identity`]).
//! 4. The web identity token must validate against the allowlisted issuers
//!    ([`OidcValidator`]): `InvalidIdentityToken`, `ExpiredTokenException`,
//!    or `IDPCommunicationError` if the issuer's keys cannot be loaded.
//! 5. The role must exist and its trust policy must allow the token's
//!    issuer, audience, subject, and authorized party; otherwise `403
//!    AccessDenied`, the same for a role that does not exist.
//! 6. The session is issued ([`crate::session`]), its record stored, and
//!    its credentials answered as AWS STS answers them.
//!
//! Errors are STS error documents (`ErrorResponse`), with the codes AWS STS
//! uses.

pub mod request;
mod response;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use aws_lc_rs::digest;
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Request, Response, StatusCode};
use s3s::Body;
use skys3_config::IdentityConfig;
use skys3_control::{ControlError, ControlStore, RetryPolicy};
use skys3_gateway::StsService;
use skys3_io::WallClock;
use skys3_types::policy::trust::WebIdentity;
use skys3_types::policy::{Policy, PolicyDocument};

pub use self::request::{AssumeRoleRequest, MAX_FORM_BYTES, MAX_SESSION_POLICY_BYTES};
use self::response::{AssumeRoleResult, error_xml};
use crate::fetch::DocumentFetcher;
use crate::identity::IdentityCopy;
use crate::session::{IssuedSession, Session, SessionGrant, SessionStore, base32};
use crate::validator::{OidcValidator, ValidationError, VerifiedToken};

/// The form content type STS requests carry.
const FORM_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";

/// The session lifetimes STS issues (`[identity]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StsSettings {
    /// `session_default_seconds`: the lifetime when a request asks for none.
    pub default_duration: Duration,
    /// `session_maximum_seconds`: the longest lifetime a request may ask
    /// for.
    pub max_duration: Duration,
}

impl StsSettings {
    /// The shortest session a request may ask for, as in AWS STS.
    pub const MIN_DURATION: Duration = Duration::from_secs(IdentityConfig::MIN_SESSION_SECONDS);

    /// The settings `[identity]` gives, or `None` if `sts_web_identity` is
    /// off and the gateway should not serve STS.
    pub fn from_config(config: &IdentityConfig) -> Option<Self> {
        config.sts_web_identity.then(|| StsSettings {
            default_duration: config.session_default(),
            max_duration: config.session_maximum(),
        })
    }
}

/// An STS error answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StsError {
    /// The HTTP status.
    pub status: StatusCode,
    /// The STS error code.
    pub code: &'static str,
    /// The message.
    pub message: String,
}

impl StsError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "ValidationError", message)
    }

    pub(crate) fn invalid_action(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidAction", message)
    }

    pub(crate) fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidParameterValue", message)
    }

    fn access_denied() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "Not authorized to perform sts:AssumeRoleWithWebIdentity",
        )
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            message,
        )
    }

    fn token(error: &ValidationError) -> Self {
        match error {
            ValidationError::Expired => Self::new(
                StatusCode::BAD_REQUEST,
                "ExpiredTokenException",
                "Token expired",
            ),
            ValidationError::KeysUnavailable(_) => Self::new(
                StatusCode::BAD_REQUEST,
                "IDPCommunicationError",
                "The identity provider's keys could not be loaded; retry later",
            ),
            other => Self::new(
                StatusCode::BAD_REQUEST,
                "InvalidIdentityToken",
                other.to_string(),
            ),
        }
    }
}

impl fmt::Display for StsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for StsError {}

/// A session STS issued, with what the response reports about it.
#[derive(Debug)]
pub struct AssumedRole {
    /// The session and its credentials.
    pub issued: IssuedSession,
    /// The verified token the session was issued for.
    pub token: VerifiedToken,
    /// `arn:aws:sts::<account>:assumed-role/<role>/<session name>`.
    pub arn: String,
    /// `<role ID>:<session name>`.
    pub assumed_role_id: String,
}

/// The STS endpoint. See the [module documentation](self).
pub struct StsEndpoint<F, S> {
    validator: OidcValidator<F>,
    identity: Arc<IdentityCopy>,
    sessions: S,
    clock: Arc<dyn WallClock>,
    settings: StsSettings,
    random: SystemRandom,
}

impl<F, S> fmt::Debug for StsEndpoint<F, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StsEndpoint")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<F: DocumentFetcher, S: SessionStore> StsEndpoint<F, S> {
    /// An endpoint that validates tokens with `validator`, finds roles in
    /// `identity`, and stores sessions in `sessions`. The validator's
    /// allowlist is replaced by the identity copy's providers on every
    /// [`StsEndpoint::sync_identity`].
    pub fn new(
        validator: OidcValidator<F>,
        identity: Arc<IdentityCopy>,
        sessions: S,
        clock: Arc<dyn WallClock>,
        settings: StsSettings,
    ) -> Self {
        Self {
            validator,
            identity,
            sessions,
            clock,
            settings,
            random: SystemRandom::new(),
        }
    }

    /// The node's identity copy.
    pub fn identity(&self) -> &Arc<IdentityCopy> {
        &self.identity
    }

    /// Syncs the identity copy from `store` ([`IdentityCopy::sync`]) and
    /// allowlists its providers. A node calls this at startup, whenever
    /// `identity/` changes, and on every configuration poll.
    ///
    /// # Errors
    ///
    /// The control store's error; the copy and the allowlist are then
    /// unchanged.
    pub async fn sync_identity<C: ControlStore>(
        &self,
        store: &C,
        retry: &RetryPolicy,
    ) -> Result<(), ControlError> {
        let snapshot = self.identity.sync(store, retry).await?;
        // The copy holds valid providers, one per issuer, so this cannot
        // fail; if it did, the old allowlist would stay.
        if let Err(error) = self
            .validator
            .set_providers(snapshot.providers().iter().cloned())
        {
            tracing::error!(%error, "cannot allowlist the identity copy's providers");
        }
        Ok(())
    }

    /// Runs `AssumeRoleWithWebIdentity`, and stores the new session.
    ///
    /// # Errors
    ///
    /// The STS error to answer with; see the [module documentation](self).
    pub async fn assume_role_with_web_identity(
        &self,
        request: &AssumeRoleRequest,
    ) -> Result<AssumedRole, StsError> {
        let duration = request
            .duration_seconds
            .map_or(self.settings.default_duration, Duration::from_secs);
        if !(StsSettings::MIN_DURATION..=self.settings.max_duration).contains(&duration) {
            return Err(StsError::validation(format!(
                "DurationSeconds must be from {} to {}",
                StsSettings::MIN_DURATION.as_secs(),
                self.settings.max_duration.as_secs()
            )));
        }
        let policy = request
            .policy
            .as_deref()
            .map(PolicyDocument::<Policy>::parse)
            .transpose()
            .map_err(|error| {
                StsError::new(
                    StatusCode::BAD_REQUEST,
                    "MalformedPolicyDocument",
                    error.to_string(),
                )
            })?;
        let snapshot = self.identity.fresh().map_err(|stale| {
            tracing::warn!("refusing a new session: {stale}");
            StsError::unavailable("The identity configuration is out of date; retry later")
        })?;
        let token = self
            .validator
            .validate(&request.token)
            .await
            .map_err(|error| {
                tracing::debug!(%error, "rejected a web identity token");
                StsError::token(&error)
            })?;
        let identity = WebIdentity {
            issuer: &token.issuer,
            audience: &token.audience,
            subject: &token.subject,
            authorized_party: token.authorized_party.as_deref(),
        };
        let allowed = snapshot
            .role(&request.role)
            .is_some_and(|role| role.trust_policy.evaluate(&identity).is_allowed());
        if !allowed {
            tracing::info!(role = %request.role, issuer = %token.issuer, subject = %token.subject,
                "denied AssumeRoleWithWebIdentity");
            return Err(StsError::access_denied());
        }
        let grant = SessionGrant {
            role: request.role.clone(),
            session_name: request.session_name.clone(),
            issuer: token.issuer.clone(),
            subject: token.subject.clone(),
            policy,
            issued_at: self.clock.now().as_secs(),
            duration,
        };
        let issued = Session::issue(grant, &self.random)
            .map_err(|error| StsError::unavailable(error.to_string()))?;
        self.sessions
            .insert(issued.session.clone())
            .await
            .map_err(|error| {
                tracing::warn!(%error, "cannot store a session");
                StsError::unavailable("The session could not be stored; retry later")
            })?;
        tracing::info!(role = %request.role, issuer = %token.issuer, subject = %token.subject,
            access_key_id = %issued.session.access_key_id, "issued a session");
        let role_digest = digest::digest(&digest::SHA256, request.role.as_bytes());
        Ok(AssumedRole {
            arn: format!(
                "arn:aws:sts::{}:assumed-role/{}/{}",
                request.account, request.role, request.session_name
            ),
            assumed_role_id: format!(
                "AROA{}:{}",
                base32(&role_digest.as_ref()[..10]),
                request.session_name
            ),
            issued,
            token,
        })
    }

    /// Answers one HTTP request.
    pub async fn handle(&self, request: Request<Body>) -> Response<Body> {
        let request_id = self.request_id();
        let (status, xml) = match self.respond(request, &request_id).await {
            Ok(xml) => (StatusCode::OK, xml),
            Err(error) => {
                let sender = error.status.is_client_error();
                let xml = error_xml(sender, error.code, &error.message, &request_id);
                (error.status, xml)
            }
        };
        let mut response = Response::new(Body::from(xml));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/xml"));
        if let Ok(id) = HeaderValue::from_str(&request_id) {
            headers.insert("x-amzn-requestid", id);
        }
        response
    }

    async fn respond(&self, request: Request<Body>, request_id: &str) -> Result<String, StsError> {
        let (parts, mut body) = request.into_parts();
        let form = parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|media| media.trim().eq_ignore_ascii_case(FORM_CONTENT_TYPE));
        if !form {
            return Err(StsError::validation(format!(
                "The request body must be {FORM_CONTENT_TYPE}"
            )));
        }
        let bytes = body.store_all_limited(MAX_FORM_BYTES).await.map_err(|_| {
            StsError::validation(format!(
                "The request body could not be read, or is longer than {MAX_FORM_BYTES} bytes"
            ))
        })?;
        let query = parts.uri.query().unwrap_or("");
        let request = request::parse(query.as_bytes(), &bytes)?;
        let assumed = self.assume_role_with_web_identity(&request).await?;
        let session = &assumed.issued.session;
        Ok(AssumeRoleResult {
            subject: &assumed.token.subject,
            audience: &assumed.token.audience,
            provider: &assumed.token.issuer,
            assumed_role_arn: &assumed.arn,
            assumed_role_id: &assumed.assumed_role_id,
            access_key_id: &session.access_key_id,
            secret_access_key: &assumed.issued.secret_access_key,
            session_token: &assumed.issued.session_token,
            expiration: session.expiration(),
            request_id,
        }
        .to_xml())
    }

    /// A random request ID in the UUID format AWS uses.
    fn request_id(&self) -> String {
        let mut bytes = [0u8; 16];
        if self.random.fill(&mut bytes).is_err() {
            return "00000000-0000-0000-0000-000000000000".to_owned();
        }
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        )
    }
}

impl<F: DocumentFetcher, S: SessionStore> StsService for StsEndpoint<F, S> {
    fn call(
        &self,
        request: Request<Body>,
    ) -> Pin<Box<dyn Future<Output = Response<Body>> + Send + '_>> {
        Box::pin(self.handle(request))
    }
}
