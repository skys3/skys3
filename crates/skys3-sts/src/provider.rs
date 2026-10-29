//! The issuer allowlist: OIDC providers whose tokens STS accepts.
//!
//! Providers are identity configuration: they live in the control store
//! under `identity/` next to roles and trust policies (design §6.1, §11),
//! and each node validates against its local copy. This type is the
//! provider record's schema, so it rejects unknown fields like every
//! control-store reader.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::jwt::Algorithm;

/// The path OIDC Discovery appends to an issuer.
const DISCOVERY_PATH: &str = "/.well-known/openid-configuration";

/// One allowlisted OIDC issuer and the claims its tokens must carry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcProvider {
    /// The issuer URL. A token's `iss` must equal it exactly, and discovery
    /// is fetched from `<issuer>/.well-known/openid-configuration`.
    pub issuer: String,
    /// The accepted audiences. A token's `aud` must contain at least one.
    pub audiences: Vec<String>,
    /// The accepted authorized parties. When not empty, a token must carry
    /// an `azp` claim equal to one of them. When empty, `azp` is not checked:
    /// in workload tokens it often names the calling workload rather than a
    /// relying party (design §11).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorized_parties: Vec<String>,
    /// The signature algorithms accepted from this issuer.
    #[serde(default = "default_algorithms")]
    pub algorithms: Vec<Algorithm>,
}

fn default_algorithms() -> Vec<Algorithm> {
    vec![Algorithm::Rs256]
}

/// Why a provider record is invalid.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ProviderError {
    /// The issuer is not an absolute `https` (or `http`) URL without
    /// credentials, query, or fragment.
    #[error("issuer {0:?} is not an absolute URL without credentials, query, or fragment")]
    InvalidIssuer(String),
    /// Two providers name the same issuer.
    #[error("issuer {0:?} is listed twice")]
    DuplicateIssuer(String),
    /// A provider accepts no audiences, or an empty one.
    #[error("issuer {0:?} must list at least one audience, none of them empty")]
    InvalidAudiences(String),
    /// A provider lists an empty authorized party.
    #[error("issuer {0:?} lists an empty authorized party")]
    InvalidAuthorizedParties(String),
    /// A provider accepts no algorithms.
    #[error("issuer {0:?} must accept at least one algorithm")]
    NoAlgorithms(String),
}

impl OidcProvider {
    /// Returns a provider for `issuer` that accepts RS256 tokens for any of
    /// `audiences` and does not check `azp`.
    pub fn new<A: Into<String>>(
        issuer: impl Into<String>,
        audiences: impl IntoIterator<Item = A>,
    ) -> Self {
        OidcProvider {
            issuer: issuer.into(),
            audiences: audiences.into_iter().map(Into::into).collect(),
            authorized_parties: Vec::new(),
            algorithms: default_algorithms(),
        }
    }

    /// Returns the OIDC Discovery URL (OpenID Connect Discovery 1.0 §4).
    pub fn discovery_url(&self) -> String {
        format!("{}{DISCOVERY_PATH}", self.issuer.trim_end_matches('/'))
    }

    /// Checks the record's own rules.
    ///
    /// # Errors
    ///
    /// Returns the first rule the record breaks.
    pub fn check(&self) -> Result<(), ProviderError> {
        let issuer = || self.issuer.clone();
        if !valid_issuer(&self.issuer) {
            return Err(ProviderError::InvalidIssuer(issuer()));
        }
        if self.audiences.is_empty() || self.audiences.iter().any(String::is_empty) {
            return Err(ProviderError::InvalidAudiences(issuer()));
        }
        if self.authorized_parties.iter().any(String::is_empty) {
            return Err(ProviderError::InvalidAuthorizedParties(issuer()));
        }
        if self.algorithms.is_empty() {
            return Err(ProviderError::NoAlgorithms(issuer()));
        }
        Ok(())
    }
}

/// Checks the OIDC issuer rules: an absolute URL with a host, and no
/// credentials, query, or fragment. Whether `http` may be fetched is the
/// fetcher's decision.
fn valid_issuer(issuer: &str) -> bool {
    if issuer.contains(['#', '?']) {
        return false;
    }
    let Ok(uri) = issuer.parse::<http::Uri>() else {
        return false;
    };
    let scheme_ok = matches!(uri.scheme_str(), Some("https" | "http"));
    let authority_ok = uri
        .authority()
        .is_some_and(|authority| !authority.as_str().contains('@') && !authority.host().is_empty());
    scheme_ok && authority_ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_url_follows_the_issuer() {
        let provider = OidcProvider::new("https://token.actions.githubusercontent.com", ["sts"]);
        assert_eq!(
            provider.discovery_url(),
            "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
        );
        let provider = OidcProvider::new("https://idp.example/tenant/", ["sts"]);
        assert_eq!(
            provider.discovery_url(),
            "https://idp.example/tenant/.well-known/openid-configuration"
        );
    }

    #[test]
    fn checks_the_record() {
        let good = OidcProvider::new("https://idp.example", ["sts"]);
        assert_eq!(good.check(), Ok(()));
        assert_eq!(
            OidcProvider::new("http://127.0.0.1:8080", ["a"]).check(),
            Ok(())
        );

        for issuer in [
            "",
            "idp.example",
            "/relative",
            "ftp://idp.example",
            "https://user@idp.example",
            "https://idp.example/?q=1",
            "https://idp.example/#f",
            "https://",
        ] {
            let provider = OidcProvider::new(issuer, ["a"]);
            assert_eq!(
                provider.check(),
                Err(ProviderError::InvalidIssuer(issuer.to_owned())),
                "{issuer}"
            );
        }
        let no_audience = OidcProvider::new("https://i", Vec::<String>::new());
        assert!(matches!(
            no_audience.check(),
            Err(ProviderError::InvalidAudiences(_))
        ));
        let empty_audience = OidcProvider::new("https://i", [""]);
        assert!(matches!(
            empty_audience.check(),
            Err(ProviderError::InvalidAudiences(_))
        ));
        let mut empty_azp = good.clone();
        empty_azp.authorized_parties = vec![String::new()];
        assert!(matches!(
            empty_azp.check(),
            Err(ProviderError::InvalidAuthorizedParties(_))
        ));
        let mut no_algorithms = good.clone();
        no_algorithms.algorithms.clear();
        let error = no_algorithms.check().unwrap_err();
        assert_eq!(
            error,
            ProviderError::NoAlgorithms("https://idp.example".into())
        );
        assert_eq!(
            error.to_string(),
            "issuer \"https://idp.example\" must accept at least one algorithm"
        );
    }

    #[test]
    fn record_schema() {
        let provider: OidcProvider = serde_json::from_str(
            r#"{"issuer":"https://i","audiences":["a"],"authorized_parties":["p"],"algorithms":["ES256","RS256"]}"#,
        )
        .unwrap();
        assert_eq!(provider.authorized_parties, ["p"]);
        assert_eq!(provider.algorithms, [Algorithm::Es256, Algorithm::Rs256]);

        let minimal: OidcProvider =
            serde_json::from_str(r#"{"issuer":"https://i","audiences":["a"]}"#).unwrap();
        assert_eq!(minimal, OidcProvider::new("https://i", ["a"]));
        assert_eq!(
            serde_json::to_string(&minimal).unwrap(),
            r#"{"issuer":"https://i","audiences":["a"],"algorithms":["RS256"]}"#
        );

        for bad in [
            r#"{"issuer":"https://i","audiences":["a"],"extra":1}"#,
            r#"{"issuer":"https://i","audiences":["a"],"algorithms":["HS256"]}"#,
            r#"{"issuer":"https://i","audiences":["a"],"algorithms":["none"]}"#,
            r#"{"audiences":["a"]}"#,
        ] {
            assert!(serde_json::from_str::<OidcProvider>(bad).is_err(), "{bad}");
        }
    }
}
