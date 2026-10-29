//! `[identity]`: anonymous access, static credentials, STS sessions, and
//! OIDC token validation (§6.2, §11).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use skys3_types::policy::Policy;

use crate::error::{Checker, key_path};

/// `[identity]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdentityConfig {
    /// `anonymous_access`: whether unsigned requests are served at all.
    /// When they are, `anonymous_policy` authorizes them.
    pub anonymous_access: bool,
    /// `anonymous_policy`: the policy, a JSON document, that authorizes
    /// unsigned requests. Required when `anonymous_access` is true, and
    /// refused otherwise.
    #[serde(deserialize_with = "optional_policy")]
    pub anonymous_policy: Option<Policy>,
    /// `[identity.static_credentials.<name>]`: access keys for bootstrap
    /// and service accounts, by principal name.
    pub static_credentials: BTreeMap<String, StaticCredentialConfig>,
    /// `sts_web_identity`: whether STS serves `AssumeRoleWithWebIdentity`.
    pub sts_web_identity: bool,
    /// `session_default_seconds`: the session lifetime when a request asks
    /// for none.
    pub session_default_seconds: u64,
    /// `session_maximum_seconds`: the longest session a request may ask
    /// for.
    pub session_maximum_seconds: u64,
    /// `identity_max_staleness_hours`: how old the node's copy of the
    /// identity configuration may be before new sessions are refused
    /// (§6.2).
    pub identity_max_staleness_hours: u64,
    /// `oidc_clock_skew_seconds`: how far the node's clock may disagree
    /// with an identity provider's when a token's `exp`, `nbf`, and `iat`
    /// are checked (§11).
    pub oidc_clock_skew_seconds: u64,
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            anonymous_access: false,
            anonymous_policy: None,
            static_credentials: BTreeMap::new(),
            sts_web_identity: true,
            session_default_seconds: 3600,
            session_maximum_seconds: 3600,
            identity_max_staleness_hours: 24,
            oidc_clock_skew_seconds: 60,
        }
    }
}

crate::durations! {
    IdentityConfig {
        /// `session_default_seconds`.
        session_default => session_default_seconds, Duration::from_secs;
        /// `session_maximum_seconds`.
        session_maximum => session_maximum_seconds, Duration::from_secs;
        /// `identity_max_staleness_hours`.
        identity_max_staleness => identity_max_staleness_hours, crate::hours;
        /// `oidc_clock_skew_seconds`.
        oidc_clock_skew => oidc_clock_skew_seconds, Duration::from_secs;
    }
}

/// `[identity.static_credentials.<name>]`: one static access key and the
/// policy that authorizes requests signed with it (§11).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticCredentialConfig {
    /// `access_key_id`: the access key ID, 16 to 128 ASCII letters and
    /// digits, unique among static credentials.
    pub access_key_id: String,
    /// `secret_access_key_file`: a file holding the secret access key. The
    /// gateway reads it at startup, so the secret never appears in the
    /// configuration.
    pub secret_access_key_file: PathBuf,
    /// `policy`: the policy, a JSON document, that authorizes the
    /// credential's requests.
    #[serde(deserialize_with = "policy")]
    pub policy: Policy,
}

impl StaticCredentialConfig {
    /// The shortest access key ID (the IAM limit).
    pub const MIN_ACCESS_KEY_ID_LEN: usize = 16;
    /// The longest access key ID (the IAM limit).
    pub const MAX_ACCESS_KEY_ID_LEN: usize = 128;
    /// The longest principal name (the IAM limit for user names).
    pub const MAX_NAME_LEN: usize = 64;
}

/// A policy given as a string that holds a JSON document.
fn policy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Policy, D::Error> {
    let json = String::deserialize(deserializer)?;
    Policy::parse(&json).map_err(serde::de::Error::custom)
}

fn optional_policy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Policy>, D::Error> {
    policy(deserializer).map(Some)
}

impl IdentityConfig {
    /// The shortest session STS issues, as in AWS STS.
    pub const MIN_SESSION_SECONDS: u64 = 900;
    /// The longest session STS issues, as in AWS STS.
    pub const MAX_SESSION_SECONDS: u64 = 43_200;
    /// The largest `oidc_clock_skew_seconds`. More would keep expired
    /// tokens usable for too long.
    pub const MAX_OIDC_CLOCK_SKEW_SECONDS: u64 = 300;

    pub(crate) fn check(&self, checker: &mut Checker) {
        self.check_anonymous(checker);
        self.check_static_credentials(checker);
        let range = Self::MIN_SESSION_SECONDS..=Self::MAX_SESSION_SECONDS;
        for (key, value) in [
            (
                "identity.session_default_seconds",
                self.session_default_seconds,
            ),
            (
                "identity.session_maximum_seconds",
                self.session_maximum_seconds,
            ),
        ] {
            checker.require(range.contains(&value), key, || {
                format!(
                    "is {value}; it must be from {} to {} (the AWS STS limits)",
                    Self::MIN_SESSION_SECONDS,
                    Self::MAX_SESSION_SECONDS
                )
            });
        }
        checker.require(
            self.session_default_seconds <= self.session_maximum_seconds,
            "identity.session_default_seconds",
            || {
                format!(
                    "is {}; it must not exceed session_maximum_seconds ({})",
                    self.session_default_seconds, self.session_maximum_seconds
                )
            },
        );
        checker.nonzero(
            "identity.identity_max_staleness_hours",
            self.identity_max_staleness_hours,
        );
        checker.require(
            self.oidc_clock_skew_seconds <= Self::MAX_OIDC_CLOCK_SKEW_SECONDS,
            "identity.oidc_clock_skew_seconds",
            || {
                format!(
                    "is {}; it must be at most {}",
                    self.oidc_clock_skew_seconds,
                    Self::MAX_OIDC_CLOCK_SKEW_SECONDS
                )
            },
        );
    }

    fn check_anonymous(&self, checker: &mut Checker) {
        match (self.anonymous_access, &self.anonymous_policy) {
            (true, None) => checker.report(
                "identity.anonymous_policy",
                "is required because anonymous_access is true",
            ),
            (false, Some(_)) => checker.report(
                "identity.anonymous_policy",
                "is set, but anonymous_access is false",
            ),
            _ => {}
        }
    }

    fn check_static_credentials(&self, checker: &mut Checker) {
        let mut key_ids = BTreeSet::new();
        for (name, credential) in &self.static_credentials {
            let table = key_path("identity.static_credentials", name);
            let name_ok = (1..=StaticCredentialConfig::MAX_NAME_LEN).contains(&name.len())
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b));
            checker.require(name_ok, &table, || {
                format!(
                    "the name must be 1 to {} ASCII letters, digits, and +=,.@_-",
                    StaticCredentialConfig::MAX_NAME_LEN
                )
            });
            let id = &credential.access_key_id;
            let id_key = format!("{table}.access_key_id");
            let lengths = StaticCredentialConfig::MIN_ACCESS_KEY_ID_LEN
                ..=StaticCredentialConfig::MAX_ACCESS_KEY_ID_LEN;
            checker.require(
                lengths.contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric()),
                &id_key,
                || {
                    format!(
                        "must be {} to {} ASCII letters and digits",
                        lengths.start(),
                        lengths.end()
                    )
                },
            );
            checker.require(key_ids.insert(id.as_str()), &id_key, || {
                format!("{id} is the access key ID of another static credential")
            });
            checker.require(
                !credential.secret_access_key_file.as_os_str().is_empty(),
                &format!("{table}.secret_access_key_file"),
                || "must not be empty".to_owned(),
            );
        }
    }
}
