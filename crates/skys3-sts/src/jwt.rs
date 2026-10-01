//! Parsing of compact JWS tokens (RFC 7515) carrying JWT claims (RFC 7519).
//!
//! Parsing checks structure only: three base64url parts, a JSON header with a
//! supported `alg` and no `crit`, and a JSON object of claims whose
//! registered claims have the types RFC 7519 gives them. Signatures, issuers,
//! audiences, and lifetimes are checked by the validator, never here, so
//! nothing a parse returns is trusted yet.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

/// The longest token accepted, in bytes: the AWS STS limit on
/// `WebIdentityToken`.
pub const MAX_TOKEN_BYTES: usize = 20_000;

/// A JWS signature algorithm that SkyS3 can verify.
///
/// Only asymmetric algorithms exist here. `none` and the HMAC algorithms
/// (`HS256` and so on) are not representable, so a token that names them is
/// rejected at parse time, and a public key can never be used as an HMAC
/// secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Algorithm {
    /// RSASSA-PKCS1-v1_5 with SHA-256.
    #[serde(rename = "RS256")]
    Rs256,
    /// RSASSA-PKCS1-v1_5 with SHA-384.
    #[serde(rename = "RS384")]
    Rs384,
    /// RSASSA-PKCS1-v1_5 with SHA-512.
    #[serde(rename = "RS512")]
    Rs512,
    /// RSASSA-PSS with SHA-256.
    #[serde(rename = "PS256")]
    Ps256,
    /// RSASSA-PSS with SHA-384.
    #[serde(rename = "PS384")]
    Ps384,
    /// RSASSA-PSS with SHA-512.
    #[serde(rename = "PS512")]
    Ps512,
    /// ECDSA on P-256 with SHA-256.
    #[serde(rename = "ES256")]
    Es256,
    /// ECDSA on P-384 with SHA-384.
    #[serde(rename = "ES384")]
    Es384,
    /// EdDSA on Ed25519.
    #[serde(rename = "EdDSA")]
    EdDsa,
}

impl Algorithm {
    /// Every supported algorithm.
    pub const ALL: [Algorithm; 9] = [
        Algorithm::Rs256,
        Algorithm::Rs384,
        Algorithm::Rs512,
        Algorithm::Ps256,
        Algorithm::Ps384,
        Algorithm::Ps512,
        Algorithm::Es256,
        Algorithm::Es384,
        Algorithm::EdDsa,
    ];

    /// Returns the algorithm's JOSE name, such as `RS256`.
    pub const fn name(self) -> &'static str {
        match self {
            Algorithm::Rs256 => "RS256",
            Algorithm::Rs384 => "RS384",
            Algorithm::Rs512 => "RS512",
            Algorithm::Ps256 => "PS256",
            Algorithm::Ps384 => "PS384",
            Algorithm::Ps512 => "PS512",
            Algorithm::Es256 => "ES256",
            Algorithm::Es384 => "ES384",
            Algorithm::EdDsa => "EdDSA",
        }
    }

    /// Returns whether the algorithm is an RSA algorithm.
    pub const fn is_rsa(self) -> bool {
        matches!(
            self,
            Algorithm::Rs256
                | Algorithm::Rs384
                | Algorithm::Rs512
                | Algorithm::Ps256
                | Algorithm::Ps384
                | Algorithm::Ps512
        )
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Algorithm {
    type Err = UnsupportedAlgorithm;

    /// Parses a JOSE algorithm name. Names are case-sensitive (RFC 7515).
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Algorithm::ALL
            .into_iter()
            .find(|alg| alg.name() == name)
            .ok_or(UnsupportedAlgorithm)
    }
}

/// The error for an algorithm name that SkyS3 does not verify.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("unsupported JWS algorithm")]
pub struct UnsupportedAlgorithm;

/// A part of a compact JWS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    /// The protected header.
    Header,
    /// The payload: the JWT claims.
    Payload,
    /// The signature.
    Signature,
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Part::Header => "header",
            Part::Payload => "payload",
            Part::Signature => "signature",
        })
    }
}

/// Why a token could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum TokenError {
    /// The token is longer than [`MAX_TOKEN_BYTES`].
    #[error("the token is longer than {MAX_TOKEN_BYTES} bytes")]
    TooLong,
    /// The token is not three `.`-separated parts.
    #[error("the token is not a compact JWS of three parts")]
    NotCompactJws,
    /// A part is not unpadded base64url.
    #[error("the token {0} is not unpadded base64url")]
    Base64(Part),
    /// The header or payload is not a JSON object.
    #[error("the token {0} is not a JSON object")]
    NotJsonObject(Part),
    /// The header has no string `alg`.
    #[error("the token header has no `alg`")]
    MissingAlgorithm,
    /// The header names an algorithm SkyS3 does not verify, such as `none`
    /// or `HS256`.
    #[error("the token is signed with an unsupported algorithm")]
    UnsupportedAlgorithm,
    /// The header has a `crit` parameter. SkyS3 implements no JWS
    /// extensions, so it must reject every token that requires one.
    #[error("the token header lists critical extensions")]
    CriticalExtension,
    /// A header parameter has the wrong type.
    #[error("the token header parameter `{0}` is invalid")]
    InvalidHeader(&'static str),
    /// A registered claim has the wrong type or value.
    #[error("the token claim `{0}` is invalid")]
    InvalidClaim(&'static str),
}

/// The protected header fields the validator uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    /// `alg`: the signature algorithm.
    pub alg: Algorithm,
    /// `kid`: the signing key's ID, if the issuer names one.
    pub kid: Option<String>,
}

/// A token's claims, with the registered claims extracted and type-checked.
#[derive(Clone, Debug, PartialEq)]
pub struct Claims {
    /// `iss`.
    pub iss: Option<String>,
    /// `sub`.
    pub sub: Option<String>,
    /// `aud`, a single string or an array of strings.
    pub aud: Vec<String>,
    /// `azp`: the authorized party.
    pub azp: Option<String>,
    /// `exp`, as time since the Unix epoch.
    pub exp: Option<Duration>,
    /// `nbf`, as time since the Unix epoch.
    pub nbf: Option<Duration>,
    /// `iat`, as time since the Unix epoch.
    pub iat: Option<Duration>,
    /// Every claim, including the ones above, as sent.
    pub all: Map<String, Value>,
}

/// A parsed token whose signature has not been checked.
#[derive(Clone, Debug)]
pub struct UnverifiedToken<'a> {
    /// The bytes the signature covers: `header.payload`, still encoded.
    pub signing_input: &'a [u8],
    /// The decoded signature.
    pub signature: Vec<u8>,
    /// The protected header.
    pub header: Header,
    /// The claims.
    pub claims: Claims,
}

impl<'a> UnverifiedToken<'a> {
    /// Parses a compact JWS token.
    ///
    /// # Errors
    ///
    /// Returns a [`TokenError`] if the token is too long or malformed, names
    /// an unsupported algorithm, or has a registered claim of the wrong
    /// type.
    pub fn parse(token: &'a str) -> Result<Self, TokenError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(TokenError::TooLong);
        }
        let mut parts = token.split('.');
        let (Some(header), Some(payload), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(TokenError::NotCompactJws);
        };
        let signing_input = &token.as_bytes()[..header.len() + 1 + payload.len()];

        let header = parse_header(&decode_object(header, Part::Header)?)?;
        let claims = parse_claims(decode_object(payload, Part::Payload)?)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| TokenError::Base64(Part::Signature))?;
        Ok(UnverifiedToken {
            signing_input,
            signature,
            header,
            claims,
        })
    }
}

fn decode_object(encoded: &str, part: Part) -> Result<Map<String, Value>, TokenError> {
    let json = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| TokenError::Base64(part))?;
    match serde_json::from_slice(&json) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(TokenError::NotJsonObject(part)),
    }
}

fn parse_header(header: &Map<String, Value>) -> Result<Header, TokenError> {
    let alg = match header.get("alg") {
        Some(Value::String(name)) => name.parse().map_err(|_| TokenError::UnsupportedAlgorithm)?,
        _ => return Err(TokenError::MissingAlgorithm),
    };
    if header.contains_key("crit") {
        return Err(TokenError::CriticalExtension);
    }
    let kid = match header.get("kid") {
        None => None,
        Some(Value::String(kid)) => Some(kid.clone()),
        Some(_) => return Err(TokenError::InvalidHeader("kid")),
    };
    Ok(Header { alg, kid })
}

fn parse_claims(all: Map<String, Value>) -> Result<Claims, TokenError> {
    Ok(Claims {
        iss: string_claim(&all, "iss")?,
        sub: string_claim(&all, "sub")?,
        aud: audience_claim(&all)?,
        azp: string_claim(&all, "azp")?,
        exp: date_claim(&all, "exp")?,
        nbf: date_claim(&all, "nbf")?,
        iat: date_claim(&all, "iat")?,
        all,
    })
}

fn string_claim(
    claims: &Map<String, Value>,
    name: &'static str,
) -> Result<Option<String>, TokenError> {
    match claims.get(name) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(TokenError::InvalidClaim(name)),
    }
}

fn audience_claim(claims: &Map<String, Value>) -> Result<Vec<String>, TokenError> {
    match claims.get("aud") {
        None => Ok(Vec::new()),
        Some(Value::String(aud)) => Ok(vec![aud.clone()]),
        Some(Value::Array(auds)) => auds
            .iter()
            .map(|aud| match aud {
                Value::String(aud) => Ok(aud.clone()),
                _ => Err(TokenError::InvalidClaim("aud")),
            })
            .collect(),
        Some(_) => Err(TokenError::InvalidClaim("aud")),
    }
}

/// Reads a NumericDate (RFC 7519 §2): seconds since the epoch, possibly
/// fractional, never negative.
fn date_claim(
    claims: &Map<String, Value>,
    name: &'static str,
) -> Result<Option<Duration>, TokenError> {
    let Some(value) = claims.get(name) else {
        return Ok(None);
    };
    let seconds = value.as_f64().ok_or(TokenError::InvalidClaim(name))?;
    Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| TokenError::InvalidClaim(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(json: &str) -> String {
        URL_SAFE_NO_PAD.encode(json)
    }

    fn token(header: &str, claims: &str) -> String {
        format!("{}.{}.c2ln", encode(header), encode(claims))
    }

    #[test]
    fn parses_a_token() {
        let text = token(
            r#"{"alg":"RS256","kid":"k1","typ":"JWT"}"#,
            r#"{"iss":"https://i","sub":"s","aud":["a","b"],"azp":"p","exp":20,"nbf":10.5,"iat":10,"x":1}"#,
        );
        let parsed = UnverifiedToken::parse(&text).unwrap();
        assert_eq!(parsed.header.alg, Algorithm::Rs256);
        assert_eq!(parsed.header.kid.as_deref(), Some("k1"));
        assert_eq!(parsed.signature, b"sig");
        assert_eq!(
            parsed.signing_input,
            text.rsplit_once('.').unwrap().0.as_bytes()
        );
        let claims = &parsed.claims;
        assert_eq!(claims.iss.as_deref(), Some("https://i"));
        assert_eq!(claims.sub.as_deref(), Some("s"));
        assert_eq!(claims.aud, ["a", "b"]);
        assert_eq!(claims.azp.as_deref(), Some("p"));
        assert_eq!(claims.exp, Some(Duration::from_secs(20)));
        assert_eq!(claims.nbf, Some(Duration::from_millis(10_500)));
        assert_eq!(claims.iat, Some(Duration::from_secs(10)));
        assert_eq!(claims.all["x"], 1);
    }

    #[test]
    fn optional_parts_may_be_absent() {
        let text = token(r#"{"alg":"EdDSA"}"#, r#"{"aud":"a"}"#);
        let parsed = UnverifiedToken::parse(&text).unwrap();
        assert_eq!(parsed.header.kid, None);
        assert_eq!(parsed.claims.aud, ["a"]);
        assert_eq!(parsed.claims.iss, None);
        assert_eq!(parsed.claims.exp, None);
        let text = token(r#"{"alg":"ES256"}"#, "{}");
        let parsed = UnverifiedToken::parse(&text).unwrap();
        assert!(parsed.claims.aud.is_empty());
    }

    #[test]
    fn rejects_unsupported_algorithms() {
        for alg in ["none", "None", "HS256", "HS512", "rs256", "RSA-OAEP", ""] {
            let text = token(&format!(r#"{{"alg":"{alg}"}}"#), "{}");
            assert_eq!(
                UnverifiedToken::parse(&text).unwrap_err(),
                TokenError::UnsupportedAlgorithm,
                "{alg}"
            );
        }
        // An unsecured JWS has an empty signature part.
        let unsecured = format!("{}.{}.", encode(r#"{"alg":"none"}"#), encode("{}"));
        assert_eq!(
            UnverifiedToken::parse(&unsecured).unwrap_err(),
            TokenError::UnsupportedAlgorithm
        );
        for header in [r#"{}"#, r#"{"alg":256}"#, r#"{"alg":null}"#] {
            assert_eq!(
                UnverifiedToken::parse(&token(header, "{}")).unwrap_err(),
                TokenError::MissingAlgorithm
            );
        }
    }

    #[test]
    fn rejects_malformed_structure() {
        let good = token(r#"{"alg":"RS256"}"#, "{}");
        let (head, _) = good.split_once('.').unwrap();
        for (text, error) in [
            (String::new(), TokenError::NotCompactJws),
            ("a.b".to_owned(), TokenError::NotCompactJws),
            (format!("{good}.x"), TokenError::NotCompactJws),
            (
                format!("{head}.{}.!!", encode("{}")),
                TokenError::Base64(Part::Signature),
            ),
            (
                format!("{head}.e30=.c2ln"),
                TokenError::Base64(Part::Payload),
            ),
            (format!("{head}.e3.c2ln"), TokenError::Base64(Part::Payload)),
            (
                format!("*.{}.c2ln", encode("{}")),
                TokenError::Base64(Part::Header),
            ),
            (token("[]", "{}"), TokenError::NotJsonObject(Part::Header)),
            (
                token(r#"{"alg":"RS256"}"#, "1"),
                TokenError::NotJsonObject(Part::Payload),
            ),
            (
                token(r#"{"alg":"RS256"}"#, "{"),
                TokenError::NotJsonObject(Part::Payload),
            ),
            (
                token(r#"{"alg":"RS256","crit":["b64"]}"#, "{}"),
                TokenError::CriticalExtension,
            ),
            (
                token(r#"{"alg":"RS256","kid":7}"#, "{}"),
                TokenError::InvalidHeader("kid"),
            ),
            ("x".repeat(MAX_TOKEN_BYTES + 1), TokenError::TooLong),
        ] {
            assert_eq!(
                UnverifiedToken::parse(&text).unwrap_err(),
                error,
                "{text:.60}"
            );
        }
    }

    #[test]
    fn rejects_mistyped_claims() {
        for (claims, name) in [
            (r#"{"iss":1}"#, "iss"),
            (r#"{"sub":["s"]}"#, "sub"),
            (r#"{"azp":null}"#, "azp"),
            (r#"{"aud":1}"#, "aud"),
            (r#"{"aud":["a",1]}"#, "aud"),
            (r#"{"exp":"1"}"#, "exp"),
            (r#"{"exp":-1}"#, "exp"),
            (r#"{"nbf":true}"#, "nbf"),
            (r#"{"iat":1e300}"#, "iat"),
        ] {
            assert_eq!(
                UnverifiedToken::parse(&token(r#"{"alg":"RS256"}"#, claims)).unwrap_err(),
                TokenError::InvalidClaim(name),
                "{claims}"
            );
        }
    }

    #[test]
    fn algorithm_names_round_trip() {
        for alg in Algorithm::ALL {
            assert_eq!(alg.name().parse::<Algorithm>(), Ok(alg));
            assert_eq!(alg.to_string(), alg.name());
            let json = serde_json::to_string(&alg).unwrap();
            assert_eq!(json, format!("\"{}\"", alg.name()));
            assert_eq!(serde_json::from_str::<Algorithm>(&json).unwrap(), alg);
            assert_eq!(alg.is_rsa(), alg.name().starts_with(['R', 'P']));
        }
        assert!(serde_json::from_str::<Algorithm>("\"HS256\"").is_err());
        assert_eq!(
            UnsupportedAlgorithm.to_string(),
            "unsupported JWS algorithm"
        );
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_text_never_panics(text in "\\PC{0,200}") {
            let _ = UnverifiedToken::parse(&text);
        }

        #[test]
        fn arbitrary_parts_never_panic(
            header in proptest::collection::vec(proptest::num::u8::ANY, 0..100),
            payload in proptest::collection::vec(proptest::num::u8::ANY, 0..100),
        ) {
            let _ = UnverifiedToken::parse(&format!(
                "{}.{}.c2ln",
                URL_SAFE_NO_PAD.encode(header),
                URL_SAFE_NO_PAD.encode(payload)
            ));
        }

        #[test]
        fn well_formed_tokens_round_trip(
            alg in proptest::sample::select(Algorithm::ALL.to_vec()),
            kid in proptest::option::of("[ -~]{0,20}"),
            sub in "\\PC{0,30}",
            aud in proptest::collection::vec("[a-z.:/]{1,20}", 1..4),
            exp in 0u32..=u32::MAX,
            signature in proptest::collection::vec(proptest::num::u8::ANY, 0..300),
        ) {
            let mut header = serde_json::json!({"alg": alg.name()});
            if let Some(kid) = &kid {
                header["kid"] = serde_json::json!(kid);
            }
            let claims = serde_json::json!({"sub": sub, "aud": aud, "exp": exp});
            let text = format!(
                "{}.{}.{}",
                encode(&header.to_string()),
                encode(&claims.to_string()),
                URL_SAFE_NO_PAD.encode(&signature)
            );
            let parsed = UnverifiedToken::parse(&text).unwrap();
            proptest::prop_assert_eq!(parsed.header, Header { alg, kid });
            proptest::prop_assert_eq!(parsed.claims.sub, Some(sub));
            proptest::prop_assert_eq!(parsed.claims.aud, aud);
            proptest::prop_assert_eq!(parsed.claims.exp, Some(Duration::from_secs(exp.into())));
            proptest::prop_assert_eq!(parsed.signature, signature);
        }
    }

    #[test]
    fn errors_display() {
        assert_eq!(
            TokenError::Base64(Part::Signature).to_string(),
            "the token signature is not unpadded base64url"
        );
        assert_eq!(
            TokenError::NotJsonObject(Part::Payload).to_string(),
            "the token payload is not a JSON object"
        );
        assert_eq!(
            TokenError::NotJsonObject(Part::Header).to_string(),
            "the token header is not a JSON object"
        );
    }
}
