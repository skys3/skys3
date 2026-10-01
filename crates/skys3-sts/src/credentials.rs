//! The gateway's credential lookup on a node: static credentials and STS
//! sessions together (design §11).

use std::fmt;
use std::sync::Arc;

use skys3_gateway::{
    CredentialLookup, LookupError, Principal, SigningCredential, StaticCredentials,
};
use skys3_io::WallClock;

use crate::identity::IdentityCopy;
use crate::session::SessionStore;

/// Finds the signing credential of an access key among the static
/// credentials and the sessions STS issued.
///
/// A static key takes no session token. A session's key needs its token,
/// which must hash to the stored hash; the secret is then opened with it
/// ([`crate::session`]). Only then is expiry checked, so a caller without
/// the token learns nothing about the session. The session's principal is
/// `assumed-role/<role>/<session name>`, with the permissions its role has
/// in the node's current identity copy, narrowed by its session policy. A
/// session whose role is gone is no longer valid. The copy's age does not
/// matter here: sessions already issued stay valid until they expire
/// (design §6.2).
///
/// | Case | Error |
/// |---|---|
/// | No such key | [`LookupError::UnknownAccessKey`] |
/// | A token with a static key; a session key without its token, with another token, or whose role is gone | [`LookupError::InvalidToken`] |
/// | The session has expired | [`LookupError::ExpiredToken`] |
/// | The session store failed | [`LookupError::Unavailable`] |
pub struct NodeCredentials<S> {
    static_keys: StaticCredentials,
    sessions: S,
    identity: Arc<IdentityCopy>,
    clock: Arc<dyn WallClock>,
}

impl<S> NodeCredentials<S> {
    /// A lookup over `static_keys` and the sessions in `sessions`, whose
    /// roles are found in `identity`.
    pub fn new(
        static_keys: StaticCredentials,
        sessions: S,
        identity: Arc<IdentityCopy>,
        clock: Arc<dyn WallClock>,
    ) -> Self {
        Self {
            static_keys,
            sessions,
            identity,
            clock,
        }
    }
}

impl<S> fmt::Debug for NodeCredentials<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeCredentials")
            .field("static_keys", &self.static_keys)
            .finish_non_exhaustive()
    }
}

impl<S: SessionStore> CredentialLookup for NodeCredentials<S> {
    async fn lookup(
        &self,
        access_key_id: &str,
        session_token: Option<&str>,
    ) -> Result<SigningCredential, LookupError> {
        match self.static_keys.lookup(access_key_id, session_token).await {
            Err(LookupError::UnknownAccessKey) => {}
            found => return found,
        }
        let session = self
            .sessions
            .get(access_key_id)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "cannot look up a session");
                LookupError::Unavailable
            })?
            .ok_or(LookupError::UnknownAccessKey)?;
        let secret = session_token
            .and_then(|token| session.open(token))
            .ok_or(LookupError::InvalidToken)?;
        if session.is_expired(self.clock.now()) {
            return Err(LookupError::ExpiredToken);
        }
        let snapshot = self.identity.snapshot();
        let role = snapshot
            .role(&session.role)
            .ok_or(LookupError::InvalidToken)?;
        let mut permissions = role.permissions.clone();
        if let Some(policy) = &session.policy {
            permissions = permissions.with_session_policy(Arc::clone(policy.policy()));
        }
        let name = format!("assumed-role/{}/{}", session.role, session.session_name);
        Ok(SigningCredential {
            secret,
            principal: Principal::new(name, permissions),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use skys3_gateway::{Permissions, SecretAccessKey};
    use skys3_io::ManualWallClock;

    use super::*;
    use crate::session::{Session, SessionStoreError};

    /// A session store that cannot answer.
    #[derive(Clone)]
    struct Broken;

    impl SessionStore for Broken {
        async fn insert(&self, _: Session) -> Result<(), SessionStoreError> {
            Err(SessionStoreError("down".into()))
        }

        async fn get(&self, _: &str) -> Result<Option<Session>, SessionStoreError> {
            Err(SessionStoreError("down".into()))
        }

        async fn remove_expired(&self, _: Duration) -> Result<usize, SessionStoreError> {
            Err(SessionStoreError("down".into()))
        }
    }

    #[tokio::test]
    async fn static_keys_come_first_and_store_failures_are_unavailable() {
        let clock: Arc<dyn WallClock> = Arc::new(ManualWallClock::new(Duration::ZERO));
        let keys = StaticCredentials::new().with_key(
            "AKIASTATIC0000001",
            SecretAccessKey::new("0123456789abcdefghijklmnopqrstuvwxyz"),
            Principal::new("bootstrap", Permissions::new([])),
        );
        let identity = Arc::new(IdentityCopy::new(Arc::clone(&clock), Duration::ZERO));
        let lookup = NodeCredentials::new(keys, Broken, identity, clock);
        let found = lookup.lookup("AKIASTATIC0000001", None).await.unwrap();
        assert_eq!(found.principal.name(), "bootstrap");
        assert_eq!(
            lookup
                .lookup("AKIASTATIC0000001", Some("t"))
                .await
                .unwrap_err(),
            LookupError::InvalidToken
        );
        assert_eq!(
            lookup
                .lookup("ASIAAAAAAAAAAAAAAAAA", Some("t"))
                .await
                .unwrap_err(),
            LookupError::Unavailable
        );
        assert!(format!("{lookup:?}").contains("bootstrap"));
        assert!(Broken.remove_expired(Duration::ZERO).await.is_err());
    }
}
