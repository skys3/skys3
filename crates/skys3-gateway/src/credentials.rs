//! Static credentials (design §11): access keys for bootstrap and service
//! accounts, from `[identity.static_credentials.<name>]`.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use skys3_config::IdentityConfig;
use zeroize::Zeroizing;

use crate::authz::{Permissions, Principal};
use crate::sigv4::{CredentialLookup, LookupError, SecretAccessKey, SigningCredential};

/// The shortest secret access key a static credential may have, in bytes.
pub const MIN_SECRET_BYTES: usize = 32;

/// The longest secret access key a static credential may have, in bytes.
pub const MAX_SECRET_BYTES: usize = 128;

/// Why the static credentials could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CredentialError {
    /// A secret access key file could not be read.
    #[error("static credential {name}: cannot read {}: {source}", path.display())]
    Read {
        /// The credential's name.
        name: String,
        /// The file.
        path: PathBuf,
        /// The I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A secret access key file does not hold a valid secret.
    #[error("static credential {name}: {}: {reason}", path.display())]
    InvalidSecret {
        /// The credential's name.
        name: String,
        /// The file.
        path: PathBuf,
        /// What is wrong with the secret.
        reason: String,
    },
}

/// The static credentials, by access key ID.
///
/// Each credential is a principal named after its table, with the policy
/// the table gives. The secret access key is read from
/// `secret_access_key_file` when the gateway starts, so it never appears in
/// the configuration. It is held as a [`SecretAccessKey`], which zeroes
/// its memory when dropped, and the buffer the file was read into is
/// zeroed too.
///
/// Static credentials have no session token. STS sessions (plan M1-24)
/// have their own [`CredentialLookup`], which a node combines with this
/// one.
#[derive(Clone, Default)]
pub struct StaticCredentials {
    keys: HashMap<String, SigningCredential>,
}

impl StaticCredentials {
    /// No credentials.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads `[identity.static_credentials]`, reading each secret access
    /// key from its file. This reads files, so a node calls it at startup
    /// or on a blocking thread.
    ///
    /// # Errors
    ///
    /// If a file cannot be read, or does not hold 32 to 128 visible ASCII
    /// characters, optionally followed by a line ending.
    pub fn load(config: &IdentityConfig) -> Result<Self, CredentialError> {
        let mut credentials = Self::new();
        for (name, credential) in &config.static_credentials {
            let path = &credential.secret_access_key_file;
            let contents = std::fs::read(path).map(Zeroizing::new).map_err(|source| {
                CredentialError::Read {
                    name: name.clone(),
                    path: path.clone(),
                    source,
                }
            })?;
            let secret =
                parse_secret(&contents).map_err(|reason| CredentialError::InvalidSecret {
                    name: name.clone(),
                    path: path.clone(),
                    reason,
                })?;
            let permissions = Permissions::from_policy(credential.policy.clone());
            credentials = credentials.with_key(
                &credential.access_key_id,
                secret,
                Principal::new(name.as_str(), permissions),
            );
        }
        Ok(credentials)
    }

    /// Adds a key, replacing any key with the same ID. Configuration
    /// loading refuses duplicate IDs.
    #[must_use]
    pub fn with_key(
        mut self,
        access_key_id: &str,
        secret: SecretAccessKey,
        principal: Principal,
    ) -> Self {
        self.keys.insert(
            access_key_id.to_owned(),
            SigningCredential { secret, principal },
        );
        self
    }

    /// The number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether there are no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.keys
                    .iter()
                    .map(|(id, credential)| (id, credential.principal.name())),
            )
            .finish()
    }
}

impl CredentialLookup for StaticCredentials {
    async fn lookup(
        &self,
        access_key_id: &str,
        session_token: Option<&str>,
    ) -> Result<SigningCredential, LookupError> {
        let credential = self
            .keys
            .get(access_key_id)
            .ok_or(LookupError::UnknownAccessKey)?;
        if session_token.is_some() {
            // A static key has no session; a token names one it is not.
            return Err(LookupError::InvalidToken);
        }
        Ok(credential.clone())
    }
}

/// The secret in a secret access key file's contents.
fn parse_secret(contents: &[u8]) -> Result<SecretAccessKey, String> {
    let secret = contents
        .strip_suffix(b"\r\n")
        .or_else(|| contents.strip_suffix(b"\n"))
        .unwrap_or(contents);
    if !(MIN_SECRET_BYTES..=MAX_SECRET_BYTES).contains(&secret.len()) {
        return Err(format!(
            "the secret is {} bytes; it must be {MIN_SECRET_BYTES} to {MAX_SECRET_BYTES}",
            secret.len()
        ));
    }
    if !secret.iter().all(u8::is_ascii_graphic) {
        return Err("the secret must be visible ASCII characters".to_owned());
    }
    Ok(SecretAccessKey::new(secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCD";

    #[test]
    fn secrets_must_be_long_visible_ascii() {
        for good in [
            SECRET.to_owned(),
            format!("{SECRET}\n"),
            format!("{SECRET}\r\n"),
        ] {
            let secret = parse_secret(good.as_bytes()).unwrap();
            assert_eq!(secret.expose(), SECRET.as_bytes());
        }
        for (bad, reason) in [
            ("short\n".to_owned(), "bytes"),
            ("x".repeat(129), "bytes"),
            (format!("{SECRET} "), "visible"),
            (format!(" {SECRET}"), "visible"),
            (format!("{SECRET}\n\n"), "visible"),
            (format!("{SECRET}é"), "visible"),
        ] {
            let error = parse_secret(bad.as_bytes()).unwrap_err();
            assert!(error.contains(reason), "{bad:?}: {error}");
        }
    }

    #[tokio::test]
    async fn static_keys_have_no_session() {
        let credentials = StaticCredentials::new().with_key(
            "AKIASTATIC0000001",
            SecretAccessKey::new(SECRET),
            Principal::new("svc", Permissions::allow_all()),
        );
        assert_eq!(credentials.len(), 1);
        assert!(!credentials.is_empty());
        let found = credentials.lookup("AKIASTATIC0000001", None).await.unwrap();
        assert_eq!(found.principal.name(), "svc");
        assert_eq!(
            credentials
                .lookup("AKIASTATIC0000001", Some("token"))
                .await
                .unwrap_err(),
            LookupError::InvalidToken
        );
        assert_eq!(
            credentials.lookup("AKIAOTHER", None).await.unwrap_err(),
            LookupError::UnknownAccessKey
        );
        let debug = format!("{credentials:?}");
        assert!(debug.contains("svc") && !debug.contains(SECRET), "{debug}");
    }
}
