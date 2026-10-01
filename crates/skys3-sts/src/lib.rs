#![forbid(unsafe_code)]
//! Workload identity for SkyS3: validation of OIDC tokens from allowlisted
//! issuers, the STS endpoint that exchanges them for session credentials,
//! and the lookup that recognizes those credentials (design §6.2, §11).
//!
//! - [`jwt`] parses compact JWS tokens and their registered claims.
//! - [`jwk`] loads an issuer's JSON Web Key Set.
//! - [`OidcProvider`] is one allowlisted issuer: its audiences, authorized
//!   parties, and accepted algorithms. [`ProviderDocument`] is its
//!   `identity/providers/<name>.json` register.
//! - [`OidcValidator`] checks claims and signatures, and caches and rotates
//!   each issuer's keys.
//! - [`DocumentFetcher`] fetches discovery documents and key sets:
//!   [`HttpsFetcher`] on a node, [`MemoryFetcher`] in tests.
//! - [`IdentityCopy`] is the node's copy of the `identity/` registers
//!   (providers and roles), with the age that makes new sessions fail
//!   closed once it passes `identity_max_staleness`.
//! - [`StsEndpoint`] serves `AssumeRoleWithWebIdentity` on the gateway's
//!   listener ([`endpoint`]): it validates the token, evaluates the role's
//!   trust policy (`skys3_types::policy::trust`), and issues a session.
//! - [`Session`] and [`SessionStore`] are session records, which store the
//!   token hashed and the secret sealed with it ([`session`]);
//!   [`NodeCredentials`] is the gateway's credential lookup over static
//!   credentials and sessions.
//! - [`WallClock`] (from `skys3-io`) is the time source for token
//!   lifetimes, key-cache ages, session expiry, and the identity copy's
//!   age: they are Unix times, so they are compared with the node's wall
//!   clock, not its monotonic clock.
//!
//! Signatures are verified with `aws-lc-rs`, the provider rustls uses, and
//! only asymmetric algorithms exist: `none` and HMAC tokens are rejected
//! when parsed, so a public key can never serve as an HMAC secret.
//!
//! With the `test-util` feature, `testkit` makes signing keys and mints
//! tokens.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use skys3_sts::{
//!     MemoryFetcher, OidcProvider, OidcValidator, SystemWallClock, ValidationError,
//!     ValidatorSettings,
//! };
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let validator = OidcValidator::new(
//!     MemoryFetcher::new(),
//!     Arc::new(SystemWallClock),
//!     ValidatorSettings::default(),
//! );
//! validator.set_providers([OidcProvider::new(
//!     "https://token.actions.githubusercontent.com",
//!     ["sts.amazonaws.com"],
//! )])?;
//!
//! // An unsigned token is rejected before any key is fetched.
//! let unsigned = "eyJhbGciOiJub25lIn0.e30.";
//! assert!(matches!(
//!     validator.validate(unsigned).await,
//!     Err(ValidationError::Malformed(_))
//! ));
//! # Ok::<(), skys3_sts::ProviderError>(())
//! # }).unwrap();
//! ```

mod credentials;
pub mod endpoint;
pub mod fetch;
#[doc(hidden)]
pub mod fuzzing;
mod http;
pub mod identity;
pub mod jwk;
pub mod jwt;
pub mod provider;
pub mod session;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod testkit;
pub mod validator;

pub use credentials::NodeCredentials;
pub use endpoint::{AssumedRole, StsEndpoint, StsError, StsSettings};
pub use fetch::{DocumentFetcher, FetchError, MemoryFetcher};
pub use http::{HttpsFetcher, HttpsFetcherError, HttpsFetcherOptions};
pub use identity::{IdentityCopy, IdentitySnapshot, Role, StaleIdentity};
pub use jwt::Algorithm;
pub use provider::{OidcProvider, ProviderDocument, ProviderError};
pub use session::{MemorySessionStore, Session, SessionStore, SessionStoreError};
pub use skys3_io::{ManualWallClock, SystemWallClock, WallClock};
pub use validator::{
    KeySourceError, OidcValidator, ValidationError, ValidatorSettings, VerifiedToken,
};
