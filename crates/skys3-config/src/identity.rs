//! `[identity]`: anonymous access, STS sessions, and OIDC token validation
//! (§6.2, §11).

use std::time::Duration;

use serde::Deserialize;

use crate::error::Checker;

/// `[identity]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdentityConfig {
    /// `anonymous_access`: whether unsigned requests are served at all.
    pub anonymous_access: bool,
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

impl IdentityConfig {
    /// The shortest session STS issues, as in AWS STS.
    pub const MIN_SESSION_SECONDS: u64 = 900;
    /// The longest session STS issues, as in AWS STS.
    pub const MAX_SESSION_SECONDS: u64 = 43_200;
    /// The largest `oidc_clock_skew_seconds`. More would keep expired
    /// tokens usable for too long.
    pub const MAX_OIDC_CLOCK_SKEW_SECONDS: u64 = 300;

    pub(crate) fn check(&self, checker: &mut Checker) {
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
}
