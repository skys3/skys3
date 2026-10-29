use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

use super::*;
use crate::clock::ManualClock;
use crate::fetch::MemoryFetcher;
use crate::testkit::{TestKey, b64, jwks};

const ISSUER: &str = "https://idp.example";
const DISCOVERY: &str = "https://idp.example/.well-known/openid-configuration";
const JWKS: &str = "https://keys.idp.example/jwks";
const AUDIENCE: &str = "sts.skys3.example";
const NOW: u64 = 1_800_000_000;

struct Fixture {
    validator: OidcValidator<MemoryFetcher>,
    clock: ManualClock,
}

impl Fixture {
    fn new(keys: &[&TestKey]) -> Self {
        Self::with_provider(keys, provider())
    }

    fn with_provider(keys: &[&TestKey], provider: OidcProvider) -> Self {
        let fetcher = MemoryFetcher::new();
        fetcher.insert(DISCOVERY, discovery(ISSUER));
        fetcher.insert(JWKS, jwks(keys));
        let clock = ManualClock::new(Duration::from_secs(NOW));
        let validator = OidcValidator::new(
            fetcher,
            Arc::new(clock.clone()),
            ValidatorSettings::default(),
        );
        validator.set_providers([provider]).unwrap();
        Fixture { validator, clock }
    }

    fn fetcher(&self) -> &MemoryFetcher {
        self.validator.fetcher()
    }

    fn publish(&self, keys: &[&TestKey]) {
        self.fetcher().insert(JWKS, jwks(keys));
    }

    async fn validate(&self, token: &str) -> Result<VerifiedToken, ValidationError> {
        self.validator.validate(token).await
    }

    fn advance(&self, seconds: u64) {
        self.clock.advance(Duration::from_secs(seconds));
    }
}

fn provider() -> OidcProvider {
    let mut provider = OidcProvider::new(ISSUER, [AUDIENCE]);
    provider.algorithms = Algorithm::ALL.to_vec();
    provider
}

fn discovery(issuer: &str) -> String {
    json!({
        "issuer": issuer,
        "jwks_uri": JWKS,
        "id_token_signing_alg_values_supported": ["RS256"],
    })
    .to_string()
}

fn claims() -> Value {
    json!({
        "iss": ISSUER,
        "sub": "repo:example/app:ref:refs/heads/main",
        "aud": AUDIENCE,
        "iat": NOW - 10,
        "nbf": NOW - 10,
        "exp": NOW + 600,
        "repository": "example/app",
    })
}

fn with(changes: Value) -> Value {
    let mut claims = claims();
    for (name, value) in changes.as_object().unwrap() {
        if value.is_null() {
            claims.as_object_mut().unwrap().remove(name);
        } else {
            claims[name] = value.clone();
        }
    }
    claims
}

// Accepted tokens.

#[tokio::test]
async fn accepts_every_supported_algorithm() {
    for alg in Algorithm::ALL {
        let key = TestKey::for_algorithm(alg, "k1");
        let fixture = Fixture::new(&[&key]);
        let verified = fixture
            .validate(&key.sign(alg, &claims()))
            .await
            .unwrap_or_else(|e| panic!("{alg}: {e}"));
        assert_eq!(verified.algorithm, alg);
        assert_eq!(verified.issuer, ISSUER);
        assert_eq!(verified.subject, "repo:example/app:ref:refs/heads/main");
        assert_eq!(verified.audience, AUDIENCE);
        assert_eq!(verified.authorized_party, None);
        assert_eq!(verified.expires_at, Duration::from_secs(NOW + 600));
        assert_eq!(verified.claims["repository"], "example/app");
    }
}

#[tokio::test]
async fn accepts_one_matching_audience_among_several() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(
        Algorithm::Rs256,
        &with(json!({"aud": ["other", AUDIENCE, "third"]})),
    );
    assert_eq!(fixture.validate(&token).await.unwrap().audience, AUDIENCE);
}

#[tokio::test]
async fn a_token_without_kid_is_checked_against_every_fitting_key() {
    let ec = TestKey::ec(Algorithm::Es256, "ec");
    let first = TestKey::other_rsa("a");
    let signer = TestKey::rsa("b").without_kid();
    let fixture = Fixture::new(&[&ec, &first, &TestKey::rsa("b")]);
    let token = signer.sign(Algorithm::Rs256, &claims());
    fixture.validate(&token).await.unwrap();

    let forger = TestKey::ec(Algorithm::Es256, "x").without_kid();
    assert_eq!(
        fixture
            .validate(&forger.sign(Algorithm::Es256, &claims()))
            .await,
        Err(ValidationError::InvalidSignature)
    );
}

// Forged and tampered tokens.

#[tokio::test]
async fn rejects_forged_signatures() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);

    // Another key pair claiming the issuer's key ID.
    let forger = TestKey::other_rsa("k1");
    assert_eq!(
        fixture
            .validate(&forger.sign(Algorithm::Rs256, &claims()))
            .await,
        Err(ValidationError::InvalidSignature)
    );

    // A genuine signature over different claims.
    let genuine = key.sign(Algorithm::Rs256, &claims());
    let (_, rest) = genuine.split_once('.').unwrap();
    let (_, signature) = rest.split_once('.').unwrap();
    let header = genuine.split('.').next().unwrap();
    let tampered = format!(
        "{header}.{}.{signature}",
        b64(with(json!({"sub": "admin"})).to_string())
    );
    assert_eq!(
        fixture.validate(&tampered).await,
        Err(ValidationError::InvalidSignature)
    );

    // A truncated signature.
    let (input, signature) = genuine.rsplit_once('.').unwrap();
    let mut signature = URL_SAFE_NO_PAD.decode(signature).unwrap();
    signature.pop();
    let truncated = format!("{input}.{}", b64(signature));
    assert_eq!(
        fixture.validate(&truncated).await,
        Err(ValidationError::InvalidSignature)
    );

    // A PS256 signature presented as RS256 by the same key.
    let relabeled = key.sign_raw(
        Algorithm::Ps256,
        &json!({"alg": "RS256", "kid": "k1"}),
        &claims(),
    );
    assert_eq!(
        fixture.validate(&relabeled).await,
        Err(ValidationError::InvalidSignature)
    );
}

// Algorithms.

#[tokio::test]
async fn rejects_unsigned_tokens() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    for alg in ["none", "NONE", "nOnE"] {
        let header = b64(json!({"alg": alg, "kid": "k1"}).to_string());
        let token = format!("{header}.{}.", b64(claims().to_string()));
        assert_eq!(
            fixture.validate(&token).await,
            Err(ValidationError::Malformed(TokenError::UnsupportedAlgorithm)),
            "{alg}"
        );
    }
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 0);
}

#[tokio::test]
async fn rejects_hmac_signed_with_the_public_key() {
    for key in [
        TestKey::rsa("k1"),
        TestKey::ec(Algorithm::Es256, "k1"),
        TestKey::ed25519("k1"),
    ] {
        let fixture = Fixture::new(&[&key]);
        assert_eq!(
            fixture.validate(&key.hs256_confusion(&claims())).await,
            Err(ValidationError::Malformed(TokenError::UnsupportedAlgorithm))
        );
    }
}

#[tokio::test]
async fn rejects_algorithms_the_issuer_does_not_accept() {
    let key = TestKey::ec(Algorithm::Es256, "k1");
    let fixture = Fixture::with_provider(&[&key], OidcProvider::new(ISSUER, [AUDIENCE]));
    assert_eq!(
        fixture
            .validate(&key.sign(Algorithm::Es256, &claims()))
            .await,
        Err(ValidationError::AlgorithmNotAllowed(Algorithm::Es256))
    );
}

#[tokio::test]
async fn a_key_never_verifies_another_key_types_algorithm() {
    let rsa = TestKey::rsa("rsa");
    let p256 = TestKey::ec(Algorithm::Es256, "p256");
    let p384 = TestKey::ec(Algorithm::Es384, "p384");
    let ed = TestKey::ed25519("ed");
    let fixture = Fixture::new(&[&rsa, &p256, &p384, &ed]);

    // Each token names a key of the wrong type for its algorithm.
    for (signer, alg, kid) in [
        (&p256, Algorithm::Es256, "rsa"),
        (&rsa, Algorithm::Rs256, "p256"),
        (&p384, Algorithm::Es384, "p256"),
        (&p256, Algorithm::Es256, "p384"),
        (&ed, Algorithm::EdDsa, "rsa"),
        (&rsa, Algorithm::Ps256, "ed"),
    ] {
        let token = signer.sign_raw(alg, &json!({"alg": alg.name(), "kid": kid}), &claims());
        assert_eq!(
            fixture.validate(&token).await,
            Err(ValidationError::UnknownKey),
            "{alg} with {kid}"
        );
    }
}

#[tokio::test]
async fn a_jwk_alg_restricts_its_key() {
    let key = TestKey::rsa("k1");
    let mut jwk = key.jwk();
    jwk["alg"] = json!("RS256");
    let fixture = Fixture::new(&[]);
    fixture
        .fetcher()
        .insert(JWKS, json!({"keys": [jwk]}).to_string());
    fixture
        .validate(&key.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap();
    fixture.advance(60);
    assert_eq!(
        fixture
            .validate(&key.sign(Algorithm::Ps256, &claims()))
            .await,
        Err(ValidationError::UnknownKey)
    );
}

// Lifetimes.

#[tokio::test]
async fn rejects_expired_tokens() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = |exp: u64| {
        key.sign(
            Algorithm::Rs256,
            &with(json!({"exp": exp, "iat": null, "nbf": null})),
        )
    };

    // The 60-second default skew applies.
    fixture.validate(&token(NOW - 59)).await.unwrap();
    for exp in [NOW - 60, NOW - 3600, 0] {
        assert_eq!(
            fixture.validate(&token(exp)).await,
            Err(ValidationError::Expired)
        );
    }
    // A token that expires while cached keys stay valid.
    let valid = key.sign(Algorithm::Rs256, &claims());
    fixture.validate(&valid).await.unwrap();
    fixture.advance(660);
    assert_eq!(
        fixture.validate(&valid).await,
        Err(ValidationError::Expired)
    );

    let no_exp = key.sign(Algorithm::Rs256, &with(json!({"exp": null})));
    assert_eq!(
        fixture.validate(&no_exp).await,
        Err(ValidationError::MissingClaim("exp"))
    );
}

#[tokio::test]
async fn rejects_tokens_that_are_not_valid_yet() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let sign = |changes| key.sign(Algorithm::Rs256, &with(changes));

    fixture
        .validate(&sign(json!({"nbf": NOW + 60})))
        .await
        .unwrap();
    fixture
        .validate(&sign(json!({"iat": NOW + 60})))
        .await
        .unwrap();
    fixture
        .validate(&sign(json!({"nbf": null, "iat": null})))
        .await
        .unwrap();
    for changes in [
        json!({"nbf": NOW + 61}),
        json!({"iat": NOW + 61}),
        json!({"nbf": NOW + 86_400, "exp": NOW + 90_000}),
    ] {
        assert_eq!(
            fixture.validate(&sign(changes.clone())).await,
            Err(ValidationError::NotYetValid),
            "{changes}"
        );
    }
}

#[tokio::test]
async fn clock_skew_is_configurable() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let validator = OidcValidator::new(
        MemoryFetcher::new(),
        Arc::new(fixture.clock.clone()),
        ValidatorSettings {
            clock_skew: Duration::ZERO,
            ..ValidatorSettings::default()
        },
    );
    validator.set_providers([provider()]).unwrap();
    validator.fetcher().insert(DISCOVERY, discovery(ISSUER));
    validator.fetcher().insert(JWKS, jwks(&[&key]));

    let expired = key.sign(Algorithm::Rs256, &with(json!({"exp": NOW})));
    let early = key.sign(Algorithm::Rs256, &with(json!({"nbf": NOW + 1})));
    fixture.validate(&expired).await.unwrap();
    fixture.validate(&early).await.unwrap();
    assert_eq!(
        validator.validate(&expired).await,
        Err(ValidationError::Expired)
    );
    assert_eq!(
        validator.validate(&early).await,
        Err(ValidationError::NotYetValid)
    );
}

// Issuer, subject, audience, and authorized party.

#[tokio::test]
async fn rejects_issuers_that_are_not_allowlisted() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    for iss in [
        json!("https://evil.example"),
        json!("https://idp.example/"),
        json!(null),
    ] {
        let token = key.sign(Algorithm::Rs256, &with(json!({"iss": iss.clone()})));
        let expected = if iss.is_null() {
            ValidationError::MissingClaim("iss")
        } else {
            ValidationError::UnknownIssuer
        };
        assert_eq!(fixture.validate(&token).await, Err(expected), "{iss}");
    }
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 0);
}

#[tokio::test]
async fn rejects_wrong_or_missing_audiences() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    for (aud, expected) in [
        (json!("sts.amazonaws.com"), ValidationError::WrongAudience),
        (json!(["a", "b"]), ValidationError::WrongAudience),
        (json!("STS.SKYS3.EXAMPLE"), ValidationError::WrongAudience),
        (json!([]), ValidationError::MissingClaim("aud")),
        (json!(null), ValidationError::MissingClaim("aud")),
    ] {
        let token = key.sign(Algorithm::Rs256, &with(json!({"aud": aud.clone()})));
        assert_eq!(fixture.validate(&token).await, Err(expected), "{aud}");
    }
}

#[tokio::test]
async fn requires_a_subject() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    for sub in [json!(null), json!("")] {
        let token = key.sign(Algorithm::Rs256, &with(json!({"sub": sub})));
        assert_eq!(
            fixture.validate(&token).await,
            Err(ValidationError::MissingClaim("sub"))
        );
    }
}

#[tokio::test]
async fn checks_azp_only_when_the_issuer_lists_authorized_parties() {
    let key = TestKey::rsa("k1");

    // Not listed: azp is reported but not checked (workload tokens put the
    // caller there).
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(Algorithm::Rs256, &with(json!({"azp": "some-workload"})));
    let verified = fixture.validate(&token).await.unwrap();
    assert_eq!(verified.authorized_party.as_deref(), Some("some-workload"));

    // Listed: azp is required and must match.
    let mut strict = provider();
    strict.authorized_parties = vec!["ci-runner".into()];
    let fixture = Fixture::with_provider(&[&key], strict);
    let sign = |azp| key.sign(Algorithm::Rs256, &with(json!({"azp": azp})));
    fixture.validate(&sign(json!("ci-runner"))).await.unwrap();
    assert_eq!(
        fixture.validate(&sign(json!("some-workload"))).await,
        Err(ValidationError::WrongAuthorizedParty)
    );
    assert_eq!(
        fixture.validate(&sign(json!(null))).await,
        Err(ValidationError::MissingClaim("azp"))
    );
}

// Keys: discovery, limits, rotation, and caching.

#[tokio::test]
async fn rejects_oversized_documents() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(Algorithm::Rs256, &claims());
    let limit = ValidatorSettings::default().max_document_bytes;

    let padding = "x".repeat(limit);
    let oversized = json!({"keys": [key.jwk()], "padding": padding}).to_string();
    fixture.fetcher().insert(JWKS, oversized);
    assert_eq!(
        fixture.validate(&token).await,
        Err(ValidationError::KeysUnavailable(KeySourceError::Fetch {
            url: JWKS.into(),
            error: FetchError::TooLarge { limit },
        }))
    );

    let fixture = Fixture::new(&[&key]);
    let oversized = json!({"issuer": ISSUER, "jwks_uri": JWKS, "padding": padding}).to_string();
    fixture.fetcher().insert(DISCOVERY, oversized);
    assert!(matches!(
        fixture.validate(&token).await,
        Err(ValidationError::KeysUnavailable(KeySourceError::Fetch {
            error: FetchError::TooLarge { .. },
            ..
        }))
    ));
}

/// A fetcher that ignores the size limit, to show the validator enforces it
/// on its own.
#[derive(Debug)]
struct Unbounded(MemoryFetcher);

impl DocumentFetcher for Unbounded {
    async fn fetch(&self, url: &str, _max_bytes: usize) -> Result<Bytes, FetchError> {
        self.0.fetch(url, usize::MAX).await
    }
}

#[tokio::test]
async fn the_size_limit_does_not_depend_on_the_fetcher() {
    let key = TestKey::rsa("k1");
    let inner = MemoryFetcher::new();
    inner.insert(DISCOVERY, discovery(ISSUER));
    inner.insert(JWKS, jwks(&[&key]));
    let settings = ValidatorSettings {
        max_document_bytes: 256,
        ..ValidatorSettings::default()
    };
    let validator = OidcValidator::new(
        Unbounded(inner),
        Arc::new(ManualClock::new(Duration::from_secs(NOW))),
        settings,
    );
    validator.set_providers([provider()]).unwrap();
    assert_eq!(
        validator
            .validate(&key.sign(Algorithm::Rs256, &claims()))
            .await,
        Err(ValidationError::KeysUnavailable(KeySourceError::Fetch {
            url: JWKS.into(),
            error: FetchError::TooLarge { limit: 256 },
        }))
    );
}

#[tokio::test]
async fn discovery_must_name_the_issuer() {
    let key = TestKey::rsa("k1");
    let token = key.sign(Algorithm::Rs256, &claims());
    for (document, expected) in [
        (
            discovery("https://evil.example"),
            KeySourceError::IssuerMismatch("https://evil.example".into()),
        ),
        (
            json!({"issuer": ISSUER}).to_string(),
            KeySourceError::InvalidDiscovery,
        ),
        ("<html>".to_owned(), KeySourceError::InvalidDiscovery),
    ] {
        let fixture = Fixture::new(&[&key]);
        fixture.fetcher().insert(DISCOVERY, document);
        assert_eq!(
            fixture.validate(&token).await,
            Err(ValidationError::KeysUnavailable(expected))
        );
    }
}

#[tokio::test]
async fn reports_invalid_key_sets() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    fixture.fetcher().insert(JWKS, r#"{"keys": {}}"#);
    let error = fixture
        .validate(&key.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ValidationError::KeysUnavailable(KeySourceError::Jwks(JwksError::Malformed))
    );
    assert_eq!(
        error.to_string(),
        "the issuer's keys are unavailable: the JWKS is not a JSON object with a `keys` array"
    );
}

#[tokio::test]
async fn an_empty_key_set_matches_no_token() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[]);
    assert_eq!(
        fixture
            .validate(&key.sign(Algorithm::Rs256, &claims()))
            .await,
        Err(ValidationError::UnknownKey)
    );
}

#[tokio::test]
async fn refetches_keys_on_an_unknown_kid() {
    let old = TestKey::rsa("2025");
    let new = TestKey::other_rsa("2026");
    let fixture = Fixture::new(&[&old]);
    fixture
        .validate(&old.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 1);

    // The issuer rotates. The first token signed by the new key triggers one
    // refresh, which is not rate-limited because the last fetch was long
    // enough ago.
    fixture.advance(30);
    fixture.publish(&[&old, &new]);
    fixture
        .validate(&new.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 2);
    fixture
        .validate(&old.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap();
    // Discovery is not repeated for a key refresh.
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 1);
}

#[tokio::test]
async fn unknown_kid_refreshes_are_rate_limited() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    fixture
        .validate(&key.sign(Algorithm::Rs256, &claims()))
        .await
        .unwrap();

    let stranger = TestKey::other_rsa("made-up");
    for _ in 0..100 {
        assert_eq!(
            fixture
                .validate(&stranger.sign(Algorithm::Rs256, &claims()))
                .await,
            Err(ValidationError::UnknownKey)
        );
    }
    // The first attempt was within 30 s of the initial fetch.
    assert_eq!(fixture.fetcher().requests(JWKS), 1);

    fixture.advance(30);
    for _ in 0..100 {
        let _ = fixture
            .validate(&stranger.sign(Algorithm::Rs256, &claims()))
            .await;
    }
    assert_eq!(fixture.fetcher().requests(JWKS), 2);
}

#[tokio::test]
async fn refreshes_keys_after_their_ttl() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = |fixture: &Fixture| {
        let now = fixture.clock.now().as_secs();
        key.sign(
            Algorithm::Rs256,
            &with(json!({"iat": now, "nbf": now, "exp": now + 60})),
        )
    };
    fixture.validate(&token(&fixture)).await.unwrap();
    fixture.advance(3599);
    fixture.validate(&token(&fixture)).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 1);
    fixture.advance(1);
    fixture.validate(&token(&fixture)).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 2);
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 1);

    // Discovery is repeated once a day.
    fixture.advance(86_400);
    fixture.validate(&token(&fixture)).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 3);
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 2);
}

#[tokio::test]
async fn keeps_cached_keys_through_an_outage_up_to_max_key_age() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = |fixture: &Fixture| {
        let now = fixture.clock.now().as_secs();
        key.sign(
            Algorithm::Rs256,
            &with(json!({"iat": now, "nbf": now, "exp": now + 60})),
        )
    };
    fixture.validate(&token(&fixture)).await.unwrap();

    fixture.fetcher().fail(JWKS, FetchError::Status(503));
    fixture.advance(3600);
    fixture.validate(&token(&fixture)).await.unwrap();
    // Within the refresh interval, no new attempt is made.
    fixture.validate(&token(&fixture)).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 2);
    // A failed key fetch makes the next refresh repeat discovery.
    fixture.advance(30);
    fixture.validate(&token(&fixture)).await.unwrap();
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 2);

    fixture.advance(86_400 - 3630);
    assert_eq!(
        fixture.validate(&token(&fixture)).await,
        Err(ValidationError::KeysUnavailable(KeySourceError::Fetch {
            url: JWKS.into(),
            error: FetchError::Status(503),
        }))
    );

    // Once the issuer recovers, keys load again.
    fixture.publish(&[&key]);
    fixture.advance(30);
    fixture.validate(&token(&fixture)).await.unwrap();
}

#[tokio::test]
async fn a_failed_first_fetch_is_rate_limited_too() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    fixture.fetcher().fail(DISCOVERY, FetchError::Timeout);
    let token = key.sign(Algorithm::Rs256, &claims());
    assert_eq!(
        fixture.validate(&token).await,
        Err(ValidationError::KeysUnavailable(KeySourceError::Fetch {
            url: DISCOVERY.into(),
            error: FetchError::Timeout,
        }))
    );
    assert_eq!(
        fixture.validate(&token).await,
        Err(ValidationError::KeysUnavailable(
            KeySourceError::RateLimited
        ))
    );
    assert_eq!(fixture.fetcher().requests(DISCOVERY), 1);
}

#[tokio::test]
async fn a_clock_stepped_back_refreshes_early() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(Algorithm::Rs256, &with(json!({"iat": null, "nbf": null})));
    fixture.validate(&token).await.unwrap();
    fixture.clock.set(Duration::from_secs(NOW - 3600));
    fixture.validate(&token).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 2);
}

#[tokio::test]
async fn concurrent_validations_share_one_fetch() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(Algorithm::Rs256, &claims());
    let (a, b, c) = tokio::join!(
        fixture.validate(&token),
        fixture.validate(&token),
        fixture.validate(&token)
    );
    assert!(a.is_ok() && b.is_ok() && c.is_ok());
    assert_eq!(fixture.fetcher().requests(JWKS), 1);
}

// The allowlist.

#[tokio::test]
async fn set_providers_keeps_keys_and_rejects_bad_lists() {
    let key = TestKey::rsa("k1");
    let fixture = Fixture::new(&[&key]);
    let token = key.sign(Algorithm::Rs256, &claims());
    fixture.validate(&token).await.unwrap();

    // Changing the issuer's settings keeps its cached keys.
    let mut narrowed = provider();
    narrowed.audiences = vec!["other".into()];
    fixture.validator.set_providers([narrowed]).unwrap();
    assert_eq!(
        fixture.validate(&token).await,
        Err(ValidationError::WrongAudience)
    );
    fixture.validator.set_providers([provider()]).unwrap();
    fixture.validate(&token).await.unwrap();
    assert_eq!(fixture.fetcher().requests(JWKS), 1);

    // A bad list is refused whole.
    assert_eq!(
        fixture.validator.set_providers([provider(), provider()]),
        Err(ProviderError::DuplicateIssuer(ISSUER.into()))
    );
    assert!(matches!(
        fixture
            .validator
            .set_providers([OidcProvider::new("not a url", [AUDIENCE])]),
        Err(ProviderError::InvalidIssuer(_))
    ));
    fixture.validate(&token).await.unwrap();

    // Removing the issuer rejects its tokens.
    fixture.validator.set_providers([]).unwrap();
    assert_eq!(
        fixture.validate(&token).await,
        Err(ValidationError::UnknownIssuer)
    );
}

#[test]
fn errors_display() {
    assert_eq!(
        ValidationError::AlgorithmNotAllowed(Algorithm::Es384).to_string(),
        "the issuer does not accept ES384 signatures"
    );
    assert_eq!(
        ValidationError::Malformed(TokenError::UnsupportedAlgorithm).to_string(),
        "malformed token: the token is signed with an unsupported algorithm"
    );
    assert_eq!(
        ValidationError::MissingClaim("sub").to_string(),
        "the token has no `sub` claim"
    );
    assert_eq!(
        KeySourceError::Fetch {
            url: JWKS.into(),
            error: FetchError::Timeout
        }
        .to_string(),
        "fetching https://keys.idp.example/jwks: the request timed out"
    );
    assert_eq!(
        KeySourceError::IssuerMismatch("x".into()).to_string(),
        "the discovery document names issuer \"x\""
    );
    for error in [
        ValidationError::UnknownIssuer,
        ValidationError::Expired,
        ValidationError::NotYetValid,
        ValidationError::WrongAudience,
        ValidationError::WrongAuthorizedParty,
        ValidationError::UnknownKey,
        ValidationError::InvalidSignature,
        ValidationError::KeysUnavailable(KeySourceError::RateLimited),
        ValidationError::KeysUnavailable(KeySourceError::InvalidDiscovery),
    ] {
        assert!(!error.to_string().is_empty());
    }
}
