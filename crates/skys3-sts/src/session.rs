//! Session credentials: what STS issues, and the records that let the
//! gateway recognize them (design §11).
//!
//! A session has three parts, as AWS STS sessions do: an access key ID
//! (`ASIA` and 16 base-32 characters), a secret access key (30 random
//! bytes, 40 base64url characters), and a session token (32 random bytes,
//! 43 base64url characters). The caller signs with the secret and sends the
//! access key ID and the token with every request.
//!
//! The [`Session`] record, keyed by access key ID, stores neither the token
//! nor the secret:
//!
//! - **The token is stored hashed** (SHA-256). A lookup hashes the token
//!   the request carries and compares in constant time.
//! - **The secret is stored sealed** with a key derived from the token:
//!   XORed with `HMAC-SHA256(token, "skys3 session secret")`. Each token is
//!   random and used for one secret, so the pad is a one-time key. Opening
//!   the secret needs both the record and the token: the record alone (a
//!   copy of the store) cannot sign requests, and neither can the token
//!   alone, which travels in every request and in presigned URLs. The
//!   secret cannot be derived from the token outright for the same reason.
//!
//! An opened secret is held as a [`SecretAccessKey`], zeroed when dropped;
//! intermediate buffers are zeroed too.
//!
//! Records are kept in a [`SessionStore`]. Design §11 puts them in an
//! internal, local-only system bucket that is never flushed; object
//! storage arrives with plan M1-09, so this crate defines the trait and an
//! in-memory store ([`MemorySessionStore`]), and the node backs the trait
//! with the system bucket once it exists. Records serialize as JSON for
//! that.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aws_lc_rs::rand::SecureRandom;
use aws_lc_rs::{constant_time, digest, hmac};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use skys3_gateway::SecretAccessKey;
use skys3_types::policy::{Policy, PolicyDocument};
use zeroize::Zeroizing;

/// The prefix of every session's access key ID, as in AWS STS.
pub const ACCESS_KEY_PREFIX: &str = "ASIA";

/// Random bytes in a secret access key.
const SECRET_BYTES: usize = 30;

/// Random bytes in a session token.
const TOKEN_BYTES: usize = 32;

/// Random bytes in an access key ID after its prefix: 16 base-32
/// characters.
const ACCESS_KEY_RANDOM_BYTES: usize = 10;

/// The HMAC message that derives a token's sealing pad.
const SEAL_LABEL: &[u8] = b"skys3 session secret";

/// The record of one session. See the [module documentation](self).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    /// The session's access key ID.
    pub access_key_id: String,
    /// SHA-256 of the session token.
    #[serde(with = "base64_bytes")]
    token_hash: [u8; 32],
    /// The secret access key's random bytes, sealed with the token.
    #[serde(with = "base64_bytes")]
    sealed_secret: [u8; SECRET_BYTES],
    /// The role the session assumed.
    pub role: String,
    /// The `RoleSessionName` the caller chose.
    pub session_name: String,
    /// The issuer of the caller's web identity token.
    pub issuer: String,
    /// The subject of the caller's web identity token.
    pub subject: String,
    /// The session policy the caller passed, which narrows the role's
    /// policies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyDocument<Policy>>,
    /// When the session was issued, in seconds since the Unix epoch.
    pub issued_at: u64,
    /// When the session expires, in seconds since the Unix epoch.
    pub expires_at: u64,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("access_key_id", &self.access_key_id)
            .field("role", &self.role)
            .field("session_name", &self.session_name)
            .field("subject", &self.subject)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// What the caller of a new session receives. The secret and the token
/// are zeroed when dropped.
#[derive(Clone)]
pub struct IssuedSession {
    /// The record to store.
    pub session: Session,
    /// The secret access key.
    pub secret_access_key: Zeroizing<String>,
    /// The session token.
    pub session_token: Zeroizing<String>,
}

impl fmt::Debug for IssuedSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedSession")
            .field("session", &self.session)
            .finish_non_exhaustive()
    }
}

/// Who a new session is for, and for how long.
#[derive(Debug, Clone)]
pub struct SessionGrant {
    /// The role assumed.
    pub role: String,
    /// The caller's `RoleSessionName`.
    pub session_name: String,
    /// The token's issuer.
    pub issuer: String,
    /// The token's subject.
    pub subject: String,
    /// The session policy, if the caller passed one.
    pub policy: Option<PolicyDocument<Policy>>,
    /// The issue time, in seconds since the Unix epoch.
    pub issued_at: u64,
    /// The session's lifetime.
    pub duration: Duration,
}

/// The random number generator failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the system random number generator failed")]
pub struct RandomError;

impl Session {
    /// Issues a new session for `grant`, drawing its credentials from
    /// `random`.
    ///
    /// # Errors
    ///
    /// [`RandomError`] if `random` fails.
    pub fn issue(
        grant: SessionGrant,
        random: &dyn SecureRandom,
    ) -> Result<IssuedSession, RandomError> {
        let mut key_bytes = [0; ACCESS_KEY_RANDOM_BYTES];
        let mut secret = Zeroizing::new([0; SECRET_BYTES]);
        let mut token_bytes = Zeroizing::new([0; TOKEN_BYTES]);
        for buffer in [&mut key_bytes[..], &mut secret[..], &mut token_bytes[..]] {
            random.fill(buffer).map_err(|_| RandomError)?;
        }
        let token = Zeroizing::new(URL_SAFE_NO_PAD.encode(*token_bytes));
        let pad = seal_pad(&token);
        let mut sealed_secret = [0; SECRET_BYTES];
        for ((sealed, secret), pad) in sealed_secret.iter_mut().zip(secret.iter()).zip(pad.iter()) {
            *sealed = secret ^ pad;
        }
        let session = Session {
            access_key_id: format!("{ACCESS_KEY_PREFIX}{}", base32(&key_bytes)),
            token_hash: token_hash(&token),
            sealed_secret,
            role: grant.role,
            session_name: grant.session_name,
            issuer: grant.issuer,
            subject: grant.subject,
            policy: grant.policy,
            issued_at: grant.issued_at,
            expires_at: grant.issued_at.saturating_add(grant.duration.as_secs()),
        };
        Ok(IssuedSession {
            session,
            secret_access_key: Zeroizing::new(URL_SAFE_NO_PAD.encode(*secret)),
            session_token: token,
        })
    }

    /// Opens the session's secret access key with the session `token`, or
    /// returns `None` if `token` is not the session's.
    pub fn open(&self, token: &str) -> Option<SecretAccessKey> {
        constant_time::verify_slices_are_equal(&token_hash(token), &self.token_hash).ok()?;
        let pad = seal_pad(token);
        let mut secret = Zeroizing::new([0; SECRET_BYTES]);
        for ((secret, sealed), pad) in secret
            .iter_mut()
            .zip(self.sealed_secret.iter())
            .zip(pad.iter())
        {
            *secret = sealed ^ pad;
        }
        let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(*secret));
        Some(SecretAccessKey::new(encoded.as_bytes()))
    }

    /// When the session expires, as time since the Unix epoch.
    pub fn expiration(&self) -> Duration {
        Duration::from_secs(self.expires_at)
    }

    /// Whether the session has expired at `now`, time since the Unix
    /// epoch.
    pub fn is_expired(&self, now: Duration) -> bool {
        now >= self.expiration()
    }
}

fn token_hash(token: &str) -> [u8; 32] {
    let mut hash = [0; 32];
    hash.copy_from_slice(digest::digest(&digest::SHA256, token.as_bytes()).as_ref());
    hash
}

/// The one-time pad that seals a session's secret: `HMAC-SHA256(token,
/// SEAL_LABEL)`.
fn seal_pad(token: &str) -> Zeroizing<[u8; 32]> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, token.as_bytes());
    let mut pad = Zeroizing::new([0; 32]);
    pad.copy_from_slice(hmac::sign(&key, SEAL_LABEL).as_ref());
    pad
}

/// RFC 4648 base 32 without padding, for a whole number of 5-byte groups.
pub(crate) fn base32(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::with_capacity(bytes.len() * 8 / 5);
    for group in bytes.chunks(5) {
        let bits = group
            .iter()
            .fold(0u64, |bits, byte| bits << 8 | u64::from(*byte))
            << (8 * (5 - group.len()));
        for index in (0..8).rev() {
            out.push(char::from(ALPHABET[((bits >> (5 * index)) & 31) as usize]));
        }
    }
    out
}

/// Fixed-size byte arrays as base64url strings.
mod base64_bytes {
    use super::{Deserialize, Deserializer, Engine, Serializer, URL_SAFE_NO_PAD};

    pub(super) fn serialize<S: Serializer, const N: usize>(
        bytes: &[u8; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[u8; N], D::Error> {
        let text = String::deserialize(deserializer)?;
        URL_SAFE_NO_PAD
            .decode(text)
            .ok()
            .and_then(|bytes| <[u8; N]>::try_from(bytes).ok())
            .ok_or_else(|| serde::de::Error::custom(format!("expected {N} bytes in base64url")))
    }
}

/// A session store failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the session store failed: {0}")]
pub struct SessionStoreError(pub String);

/// Where session records are kept, by access key ID. Clones share the
/// records, so STS and the gateway's credential lookup use one store.
///
/// Design §11 puts them in an internal, local-only system bucket; see the
/// [module documentation](self).
pub trait SessionStore: Clone + Send + Sync + 'static {
    /// Stores a new session. Access key IDs are random, so an existing
    /// record is never replaced in practice.
    ///
    /// # Errors
    ///
    /// If the record could not be stored; the session must not be handed
    /// out then.
    fn insert(
        &self,
        session: Session,
    ) -> impl Future<Output = Result<(), SessionStoreError>> + Send;

    /// The session whose access key ID is `access_key_id`.
    ///
    /// # Errors
    ///
    /// If the store cannot answer.
    fn get(
        &self,
        access_key_id: &str,
    ) -> impl Future<Output = Result<Option<Session>, SessionStoreError>> + Send;

    /// Removes every session expired at `now`, time since the Unix epoch,
    /// and returns how many it removed.
    ///
    /// # Errors
    ///
    /// If the store cannot answer.
    fn remove_expired(
        &self,
        now: Duration,
    ) -> impl Future<Output = Result<usize, SessionStoreError>> + Send;
}

/// Session records in memory. Clones share the records.
#[derive(Debug, Clone, Default)]
pub struct MemorySessionStore {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl MemorySessionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of records.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Session>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SessionStore for MemorySessionStore {
    async fn insert(&self, session: Session) -> Result<(), SessionStoreError> {
        self.lock().insert(session.access_key_id.clone(), session);
        Ok(())
    }

    async fn get(&self, access_key_id: &str) -> Result<Option<Session>, SessionStoreError> {
        Ok(self.lock().get(access_key_id).cloned())
    }

    async fn remove_expired(&self, now: Duration) -> Result<usize, SessionStoreError> {
        let mut sessions = self.lock();
        let before = sessions.len();
        sessions.retain(|_, session| !session.is_expired(now));
        Ok(before - sessions.len())
    }
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::rand::SystemRandom;

    use super::*;

    fn grant(duration: u64) -> SessionGrant {
        SessionGrant {
            role: "deployer".into(),
            session_name: "ci".into(),
            issuer: "https://idp.example".into(),
            subject: "repo:a/b".into(),
            policy: None,
            issued_at: 1_000,
            duration: Duration::from_secs(duration),
        }
    }

    #[test]
    fn credentials_have_the_aws_shapes() {
        let issued = Session::issue(grant(900), &SystemRandom::new()).unwrap();
        let session = &issued.session;
        assert_eq!(session.access_key_id.len(), 20);
        assert!(session.access_key_id.starts_with(ACCESS_KEY_PREFIX));
        assert!(
            session.access_key_id[4..]
                .bytes()
                .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
        );
        assert_eq!(issued.secret_access_key.len(), 40);
        assert_eq!(issued.session_token.len(), 43);
        assert_eq!(session.expires_at, 1_900);
        assert_eq!(session.expiration(), Duration::from_secs(1_900));
        assert!(!session.is_expired(Duration::from_secs(1_899)));
        assert!(session.is_expired(Duration::from_secs(1_900)));
        let other = Session::issue(grant(900), &SystemRandom::new()).unwrap();
        assert_ne!(other.session.access_key_id, session.access_key_id);
        assert_ne!(*other.session_token, *issued.session_token);
    }

    #[test]
    fn only_the_token_opens_the_secret() {
        let issued = Session::issue(grant(900), &SystemRandom::new()).unwrap();
        let secret = issued.session.open(&issued.session_token).unwrap();
        assert_eq!(format!("{secret:?}"), "SecretAccessKey(..)");
        let other = Session::issue(grant(900), &SystemRandom::new()).unwrap();
        assert!(issued.session.open(&other.session_token).is_none());
        assert!(issued.session.open("").is_none());
        // Neither part of the record is the secret or the token.
        let record = serde_json::to_string(&issued.session).unwrap();
        assert!(!record.contains(issued.secret_access_key.as_str()));
        assert!(!record.contains(issued.session_token.as_str()));
        let debug = format!("{issued:?}");
        assert!(!debug.contains(issued.secret_access_key.as_str()));
        assert!(!debug.contains(issued.session_token.as_str()));
    }

    #[test]
    fn records_round_trip_as_json() {
        let mut issued = Session::issue(grant(3600), &SystemRandom::new()).unwrap();
        issued.session.policy = Some(
            PolicyDocument::parse(
                r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}"#,
            )
            .unwrap(),
        );
        let json = serde_json::to_vec(&issued.session).unwrap();
        let back: Session = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, issued.session);
        assert!(back.open(&issued.session_token).is_some());

        let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        value["token_hash"] = "AAAA".into();
        assert!(serde_json::from_value::<Session>(value.clone()).is_err());
        value["token_hash"] = "!".into();
        assert!(serde_json::from_value::<Session>(value).is_err());
    }

    #[test]
    fn base32_encodes_groups() {
        assert_eq!(base32(&[0; 5]), "AAAAAAAA");
        assert_eq!(base32(&[0xff; 5]), "77777777");
        assert_eq!(base32(b"fooba"), "MZXW6YTB");
    }

    #[tokio::test]
    async fn the_memory_store_keeps_and_expires_records() {
        let store = MemorySessionStore::new();
        assert!(store.is_empty());
        let short = Session::issue(grant(900), &SystemRandom::new()).unwrap();
        let long = Session::issue(grant(3600), &SystemRandom::new()).unwrap();
        let shared = store.clone();
        shared.insert(short.session.clone()).await.unwrap();
        shared.insert(long.session.clone()).await.unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(
            shared.get(&short.session.access_key_id).await.unwrap(),
            Some(short.session.clone())
        );
        assert_eq!(shared.get("ASIAOTHER").await.unwrap(), None);
        assert_eq!(
            shared
                .remove_expired(Duration::from_secs(1_900))
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.get(&short.session.access_key_id).await.unwrap(), None);
        assert!(
            store
                .get(&long.session.access_key_id)
                .await
                .unwrap()
                .is_some()
        );
    }
}
