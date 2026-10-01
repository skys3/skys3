//! OIDC token validation against the issuer allowlist.
//!
//! [`OidcValidator::validate`] accepts a token only if all of these hold:
//!
//! 1. It parses as a compact JWS with a supported algorithm ([`crate::jwt`]).
//! 2. Its `iss` is an allowlisted issuer, and that issuer accepts its `alg`.
//! 3. `exp` is present and not past, and `nbf` and `iat` (when present) are
//!    not in the future, each with the configured clock skew.
//! 4. `sub` is present and not empty, `aud` names an accepted audience, and
//!    `azp` names an accepted authorized party if the issuer lists any.
//! 5. A key from the issuer's JWKS with the token's `kid` (any key, if the
//!    token names none) and a type that fits `alg` verifies the signature.
//!
//! Claims are checked before the signature, so a token that fails cheaply
//! never costs a signature check or a key fetch.
//!
//! **Keys.** Each issuer's JWKS is found through OIDC Discovery and cached.
//! Keys older than [`ValidatorSettings::key_ttl`] are refreshed before use.
//! A token whose `kid` is not in the cached set triggers one refresh, since
//! the issuer may have rotated its keys. Refreshes of one issuer are
//! serialized, and at most one is attempted per
//! [`ValidatorSettings::min_refresh_interval`], so a flood of tokens with
//! made-up key IDs costs one fetch per interval. If a refresh fails, cached
//! keys stay in use until they are [`ValidatorSettings::max_key_age`] old,
//! so a brief identity-provider outage does not stop STS.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::clock::WallClock;
use crate::fetch::{DocumentFetcher, FetchError};
use crate::jwk::{JwksError, KeySet};
use crate::jwt::{Algorithm, Claims, TokenError, UnverifiedToken};
use crate::provider::{OidcProvider, ProviderError};

/// Limits and cache lifetimes of an [`OidcValidator`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatorSettings {
    /// How far the node's clock may disagree with the issuer's when `exp`,
    /// `nbf`, and `iat` are checked (`[identity] oidc_clock_skew_seconds`).
    pub clock_skew: Duration,
    /// The largest discovery document or JWKS accepted, in bytes.
    pub max_document_bytes: usize,
    /// How long a fetched JWKS is used before it is refreshed.
    pub key_ttl: Duration,
    /// How long a discovered `jwks_uri` is used before discovery is fetched
    /// again.
    pub discovery_ttl: Duration,
    /// The shortest interval between two refresh attempts for one issuer.
    pub min_refresh_interval: Duration,
    /// How long cached keys stay usable while refreshes fail.
    pub max_key_age: Duration,
}

impl Default for ValidatorSettings {
    fn default() -> Self {
        ValidatorSettings {
            clock_skew: Duration::from_secs(60),
            max_document_bytes: 64 * 1024,
            key_ttl: Duration::from_secs(3600),
            discovery_ttl: Duration::from_secs(24 * 3600),
            min_refresh_interval: Duration::from_secs(30),
            max_key_age: Duration::from_secs(24 * 3600),
        }
    }
}

/// Why an issuer's keys could not be loaded.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum KeySourceError {
    /// A document could not be fetched.
    #[error("fetching {url}: {error}")]
    Fetch {
        /// The document's URL.
        url: String,
        /// Why the fetch failed.
        error: FetchError,
    },
    /// The discovery document is not a JSON object with string `issuer`
    /// and `jwks_uri` members.
    #[error("the discovery document has no valid `issuer` and `jwks_uri`")]
    InvalidDiscovery,
    /// The discovery document names a different issuer (OpenID Connect
    /// Discovery 1.0 §4.3).
    #[error("the discovery document names issuer {0:?}")]
    IssuerMismatch(String),
    /// The JWKS is invalid.
    #[error("{0}")]
    Jwks(#[from] JwksError),
    /// The last refresh attempt was less than
    /// [`ValidatorSettings::min_refresh_interval`] ago.
    #[error("the keys were refreshed moments ago")]
    RateLimited,
}

/// Why a token was rejected.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// The token is malformed or names an unsupported algorithm.
    #[error("malformed token: {0}")]
    Malformed(#[from] TokenError),
    /// A required claim is missing or empty.
    #[error("the token has no `{0}` claim")]
    MissingClaim(&'static str),
    /// The token's `iss` is not allowlisted.
    #[error("the token issuer is not allowlisted")]
    UnknownIssuer,
    /// The issuer is not allowed to sign with the token's algorithm.
    #[error("the issuer does not accept {0} signatures")]
    AlgorithmNotAllowed(Algorithm),
    /// The token's `exp` has passed.
    #[error("the token has expired")]
    Expired,
    /// The token's `nbf` or `iat` is in the future.
    #[error("the token is not valid yet")]
    NotYetValid,
    /// No audience of the token is accepted.
    #[error("the token is not for an accepted audience")]
    WrongAudience,
    /// The token's `azp` is not an accepted authorized party.
    #[error("the token's authorized party is not accepted")]
    WrongAuthorizedParty,
    /// No key of the issuer fits the token's `kid` and `alg`.
    #[error("no key of the issuer matches the token")]
    UnknownKey,
    /// The signature does not verify.
    #[error("the token signature is invalid")]
    InvalidSignature,
    /// The issuer's keys could not be loaded.
    #[error("the issuer's keys are unavailable: {0}")]
    KeysUnavailable(KeySourceError),
}

/// A token that passed validation.
#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedToken {
    /// `iss`: the allowlisted issuer.
    pub issuer: String,
    /// `sub`.
    pub subject: String,
    /// The first of the token's audiences that the issuer's entry accepts.
    pub audience: String,
    /// `azp`, if present.
    pub authorized_party: Option<String>,
    /// `exp`, as time since the Unix epoch.
    pub expires_at: Duration,
    /// The algorithm that signed the token.
    pub algorithm: Algorithm,
    /// Every claim, for trust-policy conditions.
    pub claims: Map<String, Value>,
}

/// Validates OIDC tokens against an allowlist of issuers.
///
/// Validation is safe to run concurrently from many tasks; share the
/// validator in an [`Arc`].
#[derive(Debug)]
pub struct OidcValidator<F> {
    fetcher: F,
    clock: Arc<dyn WallClock>,
    settings: ValidatorSettings,
    issuers: RwLock<Arc<HashMap<String, Arc<Issuer>>>>,
}

#[derive(Debug)]
struct Issuer {
    provider: OidcProvider,
    keys: Arc<IssuerKeys>,
}

/// An issuer's key cache. It survives changes to the issuer's other
/// settings, since the keys depend only on the issuer.
#[derive(Debug, Default)]
struct IssuerKeys {
    current: RwLock<Option<Arc<CachedKeys>>>,
    refresh: tokio::sync::Mutex<RefreshState>,
}

#[derive(Debug)]
struct CachedKeys {
    keys: KeySet,
    fetched_at: Duration,
    generation: u64,
}

#[derive(Debug, Default)]
struct RefreshState {
    /// The discovered `jwks_uri` and when discovery was fetched.
    jwks_uri: Option<(String, Duration)>,
    last_attempt: Option<Duration>,
    generation: u64,
}

impl IssuerKeys {
    fn current(&self) -> Option<Arc<CachedKeys>> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Returns how long ago `then` was, or [`Duration::MAX`] if the clock has
/// stepped back past it, so a stepped clock refreshes early, never late.
fn age(now: Duration, then: Duration) -> Duration {
    now.checked_sub(then).unwrap_or(Duration::MAX)
}

impl<F: DocumentFetcher> OidcValidator<F> {
    /// Returns a validator with an empty allowlist.
    pub fn new(fetcher: F, clock: Arc<dyn WallClock>, settings: ValidatorSettings) -> Self {
        OidcValidator {
            fetcher,
            clock,
            settings,
            issuers: RwLock::default(),
        }
    }

    /// Returns the fetcher.
    pub fn fetcher(&self) -> &F {
        &self.fetcher
    }

    /// Replaces the allowlist, for example when the node's copy of the
    /// identity configuration changes. Cached keys of issuers that stay
    /// listed are kept.
    ///
    /// # Errors
    ///
    /// Returns an error, and keeps the old allowlist, if a provider is
    /// invalid or two name the same issuer.
    pub fn set_providers(
        &self,
        providers: impl IntoIterator<Item = OidcProvider>,
    ) -> Result<(), ProviderError> {
        let old = self.issuers();
        let mut issuers = HashMap::new();
        for provider in providers {
            provider.check()?;
            if issuers.contains_key(&provider.issuer) {
                return Err(ProviderError::DuplicateIssuer(provider.issuer));
            }
            let keys = old
                .get(&provider.issuer)
                .map(|issuer| Arc::clone(&issuer.keys))
                .unwrap_or_default();
            issuers.insert(provider.issuer.clone(), Arc::new(Issuer { provider, keys }));
        }
        *self.issuers.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(issuers);
        Ok(())
    }

    fn issuers(&self) -> Arc<HashMap<String, Arc<Issuer>>> {
        Arc::clone(&self.issuers.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Validates `token` and returns its verified claims.
    ///
    /// # Errors
    ///
    /// Returns the first check the token fails; see the [module
    /// documentation](self) for the checks and their order.
    pub async fn validate(&self, token: &str) -> Result<VerifiedToken, ValidationError> {
        let token = UnverifiedToken::parse(token)?;
        let iss = token
            .claims
            .iss
            .as_deref()
            .ok_or(ValidationError::MissingClaim("iss"))?;
        let issuer = self
            .issuers()
            .get(iss)
            .cloned()
            .ok_or(ValidationError::UnknownIssuer)?;
        let provider = &issuer.provider;
        let alg = token.header.alg;
        if !provider.algorithms.contains(&alg) {
            return Err(ValidationError::AlgorithmNotAllowed(alg));
        }
        let accepted = check_claims(
            provider,
            &token.claims,
            self.clock.now(),
            self.settings.clock_skew,
        )?;
        self.verify_signature(&issuer, &token).await?;

        let claims = token.claims;
        Ok(VerifiedToken {
            issuer: provider.issuer.clone(),
            subject: accepted.subject,
            audience: accepted.audience,
            authorized_party: claims.azp,
            expires_at: accepted.expires_at,
            algorithm: alg,
            claims: claims.all,
        })
    }

    async fn verify_signature(
        &self,
        issuer: &Issuer,
        token: &UnverifiedToken<'_>,
    ) -> Result<(), ValidationError> {
        let kid = token.header.kid.as_deref();
        let alg = token.header.alg;
        let mut keys = self.usable_keys(issuer).await?;
        if keys.keys.candidates(kid, alg).next().is_none() {
            // The issuer may have rotated to a key published after the last
            // fetch.
            keys = match self.refresh(issuer, Some(keys.generation)).await {
                Ok(fresh) => fresh,
                Err(error) => {
                    tracing::debug!(
                        issuer = %issuer.provider.issuer, %error,
                        "no refresh for an unknown key"
                    );
                    return Err(ValidationError::UnknownKey);
                }
            };
        }
        let mut candidates = keys.keys.candidates(kid, alg).peekable();
        if candidates.peek().is_none() {
            return Err(ValidationError::UnknownKey);
        }
        if candidates.any(|key| key.verify(alg, token.signing_input, &token.signature)) {
            Ok(())
        } else {
            Err(ValidationError::InvalidSignature)
        }
    }

    /// Returns the issuer's keys, refreshing them if they are past their
    /// TTL, and falling back to cached keys younger than `max_key_age` if
    /// the refresh fails.
    async fn usable_keys(&self, issuer: &Issuer) -> Result<Arc<CachedKeys>, ValidationError> {
        let cached = issuer.keys.current();
        let now = self.clock.now();
        if let Some(keys) = &cached
            && age(now, keys.fetched_at) < self.settings.key_ttl
        {
            return Ok(Arc::clone(keys));
        }
        match self
            .refresh(issuer, cached.as_ref().map(|keys| keys.generation))
            .await
        {
            Ok(fresh) => Ok(fresh),
            Err(error) => match cached {
                Some(keys) if age(now, keys.fetched_at) < self.settings.max_key_age => {
                    if error != KeySourceError::RateLimited {
                        tracing::warn!(
                            issuer = %issuer.provider.issuer, %error,
                            "key refresh failed; using cached keys"
                        );
                    }
                    Ok(keys)
                }
                _ => Err(ValidationError::KeysUnavailable(error)),
            },
        }
    }

    /// Fetches the issuer's keys, unless another task replaced generation
    /// `seen` while this one waited, or the last attempt was too recent.
    async fn refresh(
        &self,
        issuer: &Issuer,
        seen: Option<u64>,
    ) -> Result<Arc<CachedKeys>, KeySourceError> {
        let mut state = issuer.keys.refresh.lock().await;
        if let Some(current) = issuer.keys.current()
            && Some(current.generation) != seen
        {
            return Ok(current);
        }
        let now = self.clock.now();
        if state
            .last_attempt
            .is_some_and(|last| age(now, last) < self.settings.min_refresh_interval)
        {
            return Err(KeySourceError::RateLimited);
        }
        state.last_attempt = Some(now);

        let jwks_uri = match &state.jwks_uri {
            Some((uri, discovered)) if age(now, *discovered) < self.settings.discovery_ttl => {
                uri.clone()
            }
            _ => {
                let uri = self.discover(&issuer.provider).await?;
                state.jwks_uri = Some((uri.clone(), now));
                uri
            }
        };
        let keys = match self.fetch(&jwks_uri).await {
            Ok(document) => KeySet::parse(&document)?,
            Err(error) => {
                // The issuer may have moved its keys: discover again next time.
                state.jwks_uri = None;
                return Err(error);
            }
        };
        tracing::debug!(
            issuer = %issuer.provider.issuer, keys = keys.len(),
            "loaded issuer keys"
        );
        state.generation += 1;
        let fresh = Arc::new(CachedKeys {
            keys,
            fetched_at: now,
            generation: state.generation,
        });
        *issuer
            .keys
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&fresh));
        Ok(fresh)
    }

    /// Fetches the issuer's discovery document and returns its `jwks_uri`.
    async fn discover(&self, provider: &OidcProvider) -> Result<String, KeySourceError> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        let document = self.fetch(&provider.discovery_url()).await?;
        let discovery: Discovery =
            serde_json::from_slice(&document).map_err(|_| KeySourceError::InvalidDiscovery)?;
        if discovery.issuer != provider.issuer {
            return Err(KeySourceError::IssuerMismatch(discovery.issuer));
        }
        Ok(discovery.jwks_uri)
    }

    async fn fetch(&self, url: &str) -> Result<Bytes, KeySourceError> {
        let limit = self.settings.max_document_bytes;
        let fetch_error = |error| KeySourceError::Fetch {
            url: url.to_owned(),
            error,
        };
        let document = self.fetcher.fetch(url, limit).await.map_err(fetch_error)?;
        if document.len() > limit {
            return Err(fetch_error(FetchError::TooLarge { limit }));
        }
        Ok(document)
    }
}

/// The claims [`check_claims`] accepted.
struct Accepted {
    subject: String,
    audience: String,
    expires_at: Duration,
}

fn check_claims(
    provider: &OidcProvider,
    claims: &Claims,
    now: Duration,
    skew: Duration,
) -> Result<Accepted, ValidationError> {
    let expires_at = claims.exp.ok_or(ValidationError::MissingClaim("exp"))?;
    if now >= expires_at.saturating_add(skew) {
        return Err(ValidationError::Expired);
    }
    let latest_start = now.saturating_add(skew);
    if [claims.nbf, claims.iat]
        .into_iter()
        .flatten()
        .any(|start| start > latest_start)
    {
        return Err(ValidationError::NotYetValid);
    }
    let subject = claims
        .sub
        .clone()
        .filter(|sub| !sub.is_empty())
        .ok_or(ValidationError::MissingClaim("sub"))?;
    if claims.aud.is_empty() {
        return Err(ValidationError::MissingClaim("aud"));
    }
    let audience = claims
        .aud
        .iter()
        .find(|aud| provider.audiences.contains(aud))
        .ok_or(ValidationError::WrongAudience)?
        .clone();
    if !provider.authorized_parties.is_empty() {
        let azp = claims
            .azp
            .as_ref()
            .ok_or(ValidationError::MissingClaim("azp"))?;
        if !provider.authorized_parties.contains(azp) {
            return Err(ValidationError::WrongAuthorizedParty);
        }
    }
    Ok(Accepted {
        subject,
        audience,
        expires_at,
    })
}

#[cfg(test)]
mod tests;
