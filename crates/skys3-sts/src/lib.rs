#![forbid(unsafe_code)]
//! Workload identity for SkyS3's STS endpoint: validation of OIDC tokens
//! from allowlisted issuers (design §11).
//!
//! `AssumeRoleWithWebIdentity` (plan M1-24) hands the caller's token to an
//! [`OidcValidator`], which checks it against the issuer allowlist and
//! returns the [`VerifiedToken`] that trust policies are evaluated against.
//!
//! - [`jwt`] parses compact JWS tokens and their registered claims.
//! - [`jwk`] loads an issuer's JSON Web Key Set.
//! - [`OidcProvider`] is one allowlisted issuer: its audiences, authorized
//!   parties, and accepted algorithms.
//! - [`OidcValidator`] checks claims and signatures, and caches and rotates
//!   each issuer's keys.
//! - [`DocumentFetcher`] fetches discovery documents and key sets:
//!   [`HttpsFetcher`] on a node, [`MemoryFetcher`] in tests.
//! - [`WallClock`] is the time source for token lifetimes.
//!
//! Signatures are verified with `aws-lc-rs`, the provider rustls uses, and
//! only asymmetric algorithms exist: `none` and HMAC tokens are rejected
//! when parsed, so a public key can never serve as an HMAC secret.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use skys3_sts::{
//!     MemoryFetcher, OidcProvider, OidcValidator, SystemClock, ValidationError,
//!     ValidatorSettings,
//! };
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let validator = OidcValidator::new(
//!     MemoryFetcher::new(),
//!     Arc::new(SystemClock),
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

pub mod clock;
pub mod fetch;
#[doc(hidden)]
pub mod fuzzing;
mod http;
pub mod jwk;
pub mod jwt;
pub mod provider;
#[cfg(test)]
mod testkit;
pub mod validator;

pub use clock::{ManualClock, SystemClock, WallClock};
pub use fetch::{DocumentFetcher, FetchError, MemoryFetcher};
pub use http::{HttpsFetcher, HttpsFetcherError, HttpsFetcherOptions};
pub use jwt::Algorithm;
pub use provider::{OidcProvider, ProviderError};
pub use validator::{
    KeySourceError, OidcValidator, ValidationError, ValidatorSettings, VerifiedToken,
};
