//! SigV4 authentication (design §11, §12): the `Authorization` header,
//! presigned URLs, session tokens, and `aws-chunked` bodies with signed
//! chunks and trailers.
//!
//! [`SigV4Authenticator`] is the gateway's [`Authenticator`]. For a signed
//! request it:
//!
//! 1. reads the signing parameters from the `Authorization` header or the
//!    `X-Amz-*` query parameters; SigV2 and SigV4a are refused;
//! 2. checks the request time against the node's [`WallClock`]: within
//!    [`MAX_CLOCK_SKEW`] for header signatures, and within the URL's
//!    lifetime, at most seven days, for presigned URLs;
//! 3. checks that `host`, every `x-amz-*` header, and every `x-skys3-*`
//!    header present is signed;
//! 4. looks up the access key, and any session token, through a
//!    [`CredentialLookup`];
//! 5. builds the canonical request from the bytes received (see
//!    `canonical`) and compares signatures in constant time;
//! 6. removes the signature, so `s3s` takes the request as anonymous, and
//!    wraps the body: a declared SHA-256 is checked when the body ends
//!    (computed on the hashing pool, if the authenticator has one), and
//!    an `aws-chunked` body is decoded, its chunk and trailer signatures
//!    checked, and its trailers published in a [`Trailers`] extension;
//! 7. records the caller in an [`Authenticated`] extension, which
//!    authorization (plan M1-07b) reads.
//!
//! An unsigned request passes as it is, without an [`Authenticated`]
//! extension, unless it carries `x-skys3-*` headers, which must be signed.
//! Whether anonymous requests are allowed is for authorization to decide.
//!
//! A wrapped body reports a failed check as a [`BodyError`] from the read
//! that finds it; the whole body is authenticated only once it has been
//! read to its end without error.

pub(crate) mod body;
pub(crate) mod canonical;
pub(crate) mod chunked;
pub(crate) mod params;
#[cfg(test)]
mod vectors;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use http::request::Parts;
use http::uri::PathAndQuery;
use http::{Request, Uri};
use s3s::{Body, S3Error, s3_error};
use skys3_io::{BlockingPool, WallClock};

pub use body::{BodyError, BodyErrorKind};
pub use chunked::{MAX_CHUNK_LINE_BYTES, MAX_TRAILER_BYTES, MAX_TRAILERS, Trailers};
pub use params::AuthMethod;

use self::body::Payload;
use self::canonical::Head;
use self::chunked::ChunkSigner;
use self::params::{PRESIGNED_PARAMS, Signed, decode_pair, query_params};
use crate::service::Authenticator;

/// How far a header-signed request's time may be from the node's clock
/// (the S3 limit).
pub const MAX_CLOCK_SKEW: Duration = Duration::from_secs(15 * 60);

/// The longest a presigned URL may be valid (the S3 limit).
pub const MAX_PRESIGNED_EXPIRY: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The service every credential scope must name.
pub const SERVICE: &str = "s3";

/// The prefix of SkyS3's own request headers, which must be signed.
pub const SKYS3_HEADER_PREFIX: &str = "x-skys3-";

/// A secret access key.
///
/// Its `Debug` output is redacted. Holding secrets with `secrecy` and
/// `zeroize` is for the credential stores (plan M1-07b, M1-24).
#[derive(Clone)]
pub struct SecretAccessKey(Box<[u8]>);

impl SecretAccessKey {
    /// Wraps a secret access key.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self(secret.into().into_boxed_slice())
    }

    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretAccessKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretAccessKey(..)")
    }
}

/// What a [`CredentialLookup`] knows about an access key: its secret, and
/// whom requests signed with it come from.
#[derive(Debug, Clone)]
pub struct SigningCredential<P> {
    /// The secret access key.
    pub secret: SecretAccessKey,
    /// The principal that authorization evaluates.
    pub principal: P,
}

/// Why a [`CredentialLookup`] found no credential.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LookupError {
    /// No such access key.
    #[error("the access key does not exist")]
    UnknownAccessKey,
    /// The session token is malformed, does not belong to the key, or is
    /// missing for a key that needs one.
    #[error("the session token is not valid")]
    InvalidToken,
    /// The session has expired.
    #[error("the session token has expired")]
    ExpiredToken,
    /// The credential store cannot answer now.
    #[error("the credential store is unavailable")]
    Unavailable,
}

impl LookupError {
    fn to_s3_error(&self) -> S3Error {
        match self {
            LookupError::UnknownAccessKey => s3_error!(
                InvalidAccessKeyId,
                "The AWS Access Key Id you provided does not exist in our records."
            ),
            LookupError::InvalidToken => s3_error!(
                InvalidToken,
                "The provided token is malformed or otherwise invalid."
            ),
            LookupError::ExpiredToken => s3_error!(ExpiredToken, "The provided token has expired."),
            LookupError::Unavailable => s3_error!(
                ServiceUnavailable,
                "Credentials cannot be checked right now; please retry."
            ),
        }
    }
}

/// Finds the signing credential of an access key: static credentials
/// (plan M1-07b) and STS sessions (plan M1-24).
pub trait CredentialLookup: Send + Sync + 'static {
    /// Whom a credential identifies, for authorization.
    type Principal: Clone + Send + Sync + 'static;

    /// Looks up `access_key_id`, with the request's session token
    /// (`x-amz-security-token` or `X-Amz-Security-Token`) if it has one.
    ///
    /// # Errors
    ///
    /// Why there is no credential.
    fn lookup(
        &self,
        access_key_id: &str,
        session_token: Option<&str>,
    ) -> impl Future<Output = Result<SigningCredential<Self::Principal>, LookupError>> + Send;
}

/// The extension a signed request carries once its signature is verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authenticated<P> {
    /// Whom the credential identifies.
    pub principal: P,
    /// The access key the request was signed with.
    pub access_key_id: String,
    /// Where the signature was.
    pub method: AuthMethod,
}

/// The SigV4 [`Authenticator`]. See the [module documentation](self).
pub struct SigV4Authenticator<L> {
    lookup: L,
    clock: Arc<dyn WallClock>,
    hashing: Option<BlockingPool>,
}

impl<L> SigV4Authenticator<L> {
    /// An authenticator that finds credentials with `lookup` and reads the
    /// time from `clock`. It hashes payloads inline until it is given a
    /// hashing pool.
    pub fn new(lookup: L, clock: Arc<dyn WallClock>) -> Self {
        Self {
            lookup,
            clock,
            hashing: None,
        }
    }

    /// Computes the SHA-256 that `x-amz-content-sha256` declares on `pool`,
    /// off the reactor (design §15). A node passes its hashing pool.
    #[must_use]
    pub fn with_hashing_pool(mut self, pool: BlockingPool) -> Self {
        self.hashing = Some(pool);
        self
    }
}

impl<L> fmt::Debug for SigV4Authenticator<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigV4Authenticator")
            .field("clock", &self.clock)
            .field("hashing", &self.hashing)
            .finish_non_exhaustive()
    }
}

impl<L: CredentialLookup> Authenticator for SigV4Authenticator<L> {
    async fn authenticate(&self, request: Request<Body>) -> Result<Request<Body>, S3Error> {
        let (mut parts, body) = request.into_parts();
        let Some(signed) = params::parse(&parts, SERVICE)? else {
            if parts
                .headers
                .keys()
                .any(|name| name.as_str().starts_with(SKYS3_HEADER_PREFIX))
            {
                return Err(not_signed());
            }
            let body = match Payload::declared(&parts.headers)? {
                Some(payload) => {
                    body::wrap(&mut parts, body, payload, None, self.hashing.as_ref())?
                }
                None => body,
            };
            return Ok(Request::from_parts(parts, body));
        };
        self.check_time(&signed)?;
        check_signed_headers(&parts, &signed)?;
        check_unique_parameters(&parts)?;
        let declared = Payload::declared(&parts.headers)?;
        let (payload, payload_hash) = match (signed.method, declared) {
            (AuthMethod::Header, None) => {
                return Err(s3_error!(
                    InvalidRequest,
                    "Missing required header for this request: x-amz-content-sha256"
                ));
            }
            (AuthMethod::Header, Some(payload)) => {
                let hash = parts.headers[body::CONTENT_SHA256].to_str().unwrap_or("");
                (payload, hash.to_owned())
            }
            (AuthMethod::Presigned, Some(Payload::Chunked { .. })) => {
                return Err(s3_error!(
                    InvalidRequest,
                    "An aws-chunked body needs a signed Authorization header."
                ));
            }
            (AuthMethod::Presigned, payload) => (
                payload.unwrap_or(Payload::Unsigned),
                "UNSIGNED-PAYLOAD".to_owned(),
            ),
        };
        let credential = self
            .lookup
            .lookup(&signed.access_key_id, signed.session_token.as_deref())
            .await
            .map_err(|error| error.to_s3_error())?;
        let key = canonical::signing_key(
            credential.secret.expose(),
            &signed.date,
            &signed.region,
            &signed.service,
        );
        let canonical_request = canonical::canonical_request(
            &Head::new(&parts),
            &signed.signed_headers,
            &payload_hash,
            signed.method == AuthMethod::Presigned,
        )
        .map_err(|missing| {
            signed.method.malformed(format!(
                "The signed header {} is not in the request.",
                missing.0
            ))
        })?;
        let scope = signed.scope();
        let string_to_sign =
            canonical::string_to_sign(&signed.timestamp, &scope, &canonical_request);
        if !canonical::verify(&key, string_to_sign.as_bytes(), &signed.signature) {
            return Err(s3_error!(
                SignatureDoesNotMatch,
                "The request signature we calculated does not match the signature you provided. \
                 Check your key and signing method."
            ));
        }
        strip_signature(&mut parts, signed.method)?;
        let signer = ChunkSigner::new(key, &signed.timestamp, &scope, signed.signature);
        let body = body::wrap(
            &mut parts,
            body,
            payload,
            Some(signer),
            self.hashing.as_ref(),
        )?;
        parts.extensions.insert(Authenticated {
            principal: credential.principal,
            access_key_id: signed.access_key_id,
            method: signed.method,
        });
        Ok(Request::from_parts(parts, body))
    }
}

impl<L> SigV4Authenticator<L> {
    fn check_time(&self, signed: &Signed) -> Result<(), S3Error> {
        let now = self.clock.now();
        match signed.expires {
            None if now.abs_diff(signed.time) > MAX_CLOCK_SKEW => Err(s3_error!(
                RequestTimeTooSkewed,
                "The difference between the request time and the server's time is too large."
            )),
            None => Ok(()),
            Some(_) if signed.time > now.saturating_add(MAX_CLOCK_SKEW) => {
                Err(s3_error!(AccessDenied, "Request is not valid yet"))
            }
            Some(expires) if now > signed.time.saturating_add(expires) => {
                Err(s3_error!(AccessDenied, "Request has expired"))
            }
            Some(_) => Ok(()),
        }
    }
}

fn not_signed() -> S3Error {
    s3_error!(
        AccessDenied,
        "There were headers present in the request which were not signed"
    )
}

/// Checks that no query parameter, decoded as `s3s` decodes it, appears
/// twice. The canonical query sorts repeated parameters by value, so their
/// order is not signed, while `s3s` keeps the order received.
fn check_unique_parameters(parts: &Parts) -> Result<(), S3Error> {
    let mut names = std::collections::HashSet::new();
    for (_, name, _) in query_params(parts.uri.query().unwrap_or("")) {
        if !names.insert(name) {
            return Err(s3_error!(
                InvalidArgument,
                "A query parameter appears more than once in a signed request."
            ));
        }
    }
    Ok(())
}

/// Checks that `host`, and every `x-amz-*` and `x-skys3-*` header the
/// request has, is signed.
fn check_signed_headers(parts: &Parts, signed: &Signed) -> Result<(), S3Error> {
    if !signed.signs("host") {
        return Err(signed.method.malformed("The host header must be signed."));
    }
    let unsigned = parts.headers.keys().any(|name| {
        let name = name.as_str();
        (name.starts_with("x-amz-") || name.starts_with(SKYS3_HEADER_PREFIX)) && !signed.signs(name)
    });
    if unsigned { Err(not_signed()) } else { Ok(()) }
}

/// Removes the signature: the `Authorization` header, or the presigned
/// URL's `X-Amz-*` parameters, keeping the rest of the query as received.
fn strip_signature(parts: &mut Parts, method: AuthMethod) -> Result<(), S3Error> {
    if method == AuthMethod::Header {
        parts.headers.remove(http::header::AUTHORIZATION);
        return Ok(());
    }
    let query = parts.uri.query().unwrap_or("");
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            !pair.is_empty() && !PRESIGNED_PARAMS.contains(&decode_pair(pair).0.as_str())
        })
        .collect();
    let mut target = parts.uri.path().to_owned();
    if !kept.is_empty() {
        target.push('?');
        target.push_str(&kept.join("&"));
    }
    let mut uri = parts.uri.clone().into_parts();
    uri.path_and_query = Some(
        PathAndQuery::try_from(target)
            .map_err(|_| s3_error!(InvalidURI, "The request target is not valid."))?,
    );
    parts.uri = Uri::from_parts(uri)
        .map_err(|_| s3_error!(InvalidURI, "The request target is not valid."))?;
    Ok(())
}

/// A [`CredentialLookup`] over a fixed set of keys, for tests and fuzzing.
/// Each principal is the key's name.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, Default)]
pub struct MemoryCredentials {
    keys: std::collections::HashMap<String, (SecretAccessKey, Option<String>)>,
}

#[cfg(any(test, feature = "test-util"))]
impl MemoryCredentials {
    /// An empty set of keys.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a key. A key with a `session_token` accepts only requests that
    /// carry that token.
    #[must_use]
    pub fn with_key(
        mut self,
        access_key_id: &str,
        secret: &str,
        session_token: Option<&str>,
    ) -> Self {
        self.keys.insert(
            access_key_id.to_owned(),
            (
                SecretAccessKey::new(secret),
                session_token.map(str::to_owned),
            ),
        );
        self
    }
}

#[cfg(any(test, feature = "test-util"))]
impl CredentialLookup for MemoryCredentials {
    type Principal = String;

    async fn lookup(
        &self,
        access_key_id: &str,
        session_token: Option<&str>,
    ) -> Result<SigningCredential<String>, LookupError> {
        let (secret, token) = self
            .keys
            .get(access_key_id)
            .ok_or(LookupError::UnknownAccessKey)?;
        if token.as_deref() != session_token {
            return Err(LookupError::InvalidToken);
        }
        Ok(SigningCredential {
            secret: secret.clone(),
            principal: access_key_id.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use skys3_io::ManualWallClock;

    use super::*;

    #[test]
    fn lookup_errors_map_to_s3_errors() {
        for (error, code) in [
            (LookupError::UnknownAccessKey, "InvalidAccessKeyId"),
            (LookupError::InvalidToken, "InvalidToken"),
            (LookupError::ExpiredToken, "ExpiredToken"),
            (LookupError::Unavailable, "ServiceUnavailable"),
        ] {
            assert_eq!(error.to_s3_error().code().as_str(), code, "{error}");
        }
    }

    #[test]
    fn secrets_stay_out_of_debug_output() {
        let secret = SecretAccessKey::new("very-secret");
        assert_eq!(format!("{secret:?}"), "SecretAccessKey(..)");
        let credentials = MemoryCredentials::new().with_key("AKID", "very-secret", None);
        let auth = SigV4Authenticator::new(credentials, Arc::new(ManualWallClock::default()));
        assert!(!format!("{auth:?}").contains("very-secret"));
    }

    #[test]
    fn presigned_signatures_are_stripped_from_the_query() {
        let strip = |uri: &str| {
            let mut parts = Request::get(uri).body(()).unwrap().into_parts().0;
            strip_signature(&mut parts, AuthMethod::Presigned).unwrap();
            parts.uri.to_string()
        };
        assert_eq!(
            strip("http://h/b/k?X-Amz-Algorithm=a&x-id=GetObject&X-Amz-Signature=s&&flag"),
            "http://h/b/k?x-id=GetObject&flag"
        );
        assert_eq!(strip("/b/k?X-Amz-Signature=s"), "/b/k");
        // Escaped names are stripped too, so s3s never sees a signature.
        assert_eq!(
            strip("/b/k?X-Amz-%53ignature=s&X%2DAmz%2DSecurity%2DToken=t&keep=%53"),
            "/b/k?keep=%53"
        );
    }
}
