//! The node's copy of the identity configuration: OIDC providers and roles
//! from the control store's `identity/` registers (design §6.2, §11).
//!
//! STS validates tokens and evaluates trust policies against this copy,
//! never against the control store itself, so it keeps working through a
//! control-store outage. A revocation made during an outage cannot reach
//! the node, though, so the copy has an age: the time since the last sync
//! that read every register. Once that age passes
//! `identity_max_staleness`, [`IdentityCopy::fresh`] refuses, and STS
//! issues no new sessions until a sync succeeds. Sessions already issued
//! stay valid until they expire: their roles are looked up in the copy
//! whatever its age.
//!
//! A sync either replaces the whole copy or leaves it as it was. A register
//! that does not parse is left out of the new copy rather than failing the
//! sync: keeping the old copy would keep the register's previous value,
//! perhaps a looser trust policy, in force until the copy went stale,
//! while leaving it out denies what it would have allowed.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use skys3_control::{
    ControlError, ControlStore, KeyPrefix, RegisterKey, RetryPolicy, TypedKey, read_with_retries,
};
use skys3_gateway::Permissions;
use skys3_io::WallClock;
use skys3_types::policy::trust::TrustPolicy;
use skys3_types::{RegisterDocument, RoleDocument};

use crate::provider::{OidcProvider, ProviderDocument};

/// The prefix of OIDC provider registers.
pub const PROVIDERS_PREFIX: &str = "identity/providers/";

/// The prefix of role registers.
pub const ROLES_PREFIX: &str = "identity/roles/";

/// The suffix every identity register's key ends with.
const JSON_SUFFIX: &str = ".json";

/// The key of the provider register `name`.
///
/// # Errors
///
/// If `name` cannot be part of a register key.
pub fn provider_key(name: &str) -> Result<TypedKey<ProviderDocument>, skys3_control::KeyError> {
    RegisterKey::new(format!("{PROVIDERS_PREFIX}{name}{JSON_SUFFIX}")).map(TypedKey::new)
}

/// The key of the register of the role `name`.
///
/// # Errors
///
/// If `name` cannot be part of a register key.
pub fn role_key(name: &str) -> Result<TypedKey<RoleDocument>, skys3_control::KeyError> {
    RegisterKey::new(format!("{ROLES_PREFIX}{name}{JSON_SUFFIX}")).map(TypedKey::new)
}

/// A role as STS uses it.
#[derive(Debug, Clone)]
pub struct Role {
    /// Who may assume the role.
    pub trust_policy: Arc<TrustPolicy>,
    /// What the role's sessions may do, before any session policy.
    pub permissions: Permissions,
}

/// One version of the identity configuration.
#[derive(Debug, Default)]
pub struct IdentitySnapshot {
    providers: Vec<OidcProvider>,
    roles: BTreeMap<String, Role>,
    synced_at: Option<Duration>,
}

impl IdentitySnapshot {
    /// The allowlisted OIDC providers, one per issuer.
    pub fn providers(&self) -> &[OidcProvider] {
        &self.providers
    }

    /// The role named `name`.
    pub fn role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }

    /// When the sync that produced this snapshot started, as time since the
    /// Unix epoch, or `None` for the empty copy a node starts with.
    pub fn synced_at(&self) -> Option<Duration> {
        self.synced_at
    }
}

/// Why no new session may be issued now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the identity configuration is older than identity_max_staleness")]
pub struct StaleIdentity;

/// The node's copy of the identity configuration. See the [module
/// documentation](self).
#[derive(Debug)]
pub struct IdentityCopy {
    clock: Arc<dyn WallClock>,
    max_staleness: Duration,
    current: RwLock<Arc<IdentitySnapshot>>,
}

impl IdentityCopy {
    /// An empty copy, which is stale until its first sync. `max_staleness`
    /// is `[identity] identity_max_staleness_hours`.
    pub fn new(clock: Arc<dyn WallClock>, max_staleness: Duration) -> Self {
        Self {
            clock,
            max_staleness,
            current: RwLock::default(),
        }
    }

    /// The current snapshot, whatever its age: for sessions already
    /// issued.
    pub fn snapshot(&self) -> Arc<IdentitySnapshot> {
        Arc::clone(&self.current.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// The current snapshot, if it is fresh enough to issue new sessions
    /// from. A clock that stepped back before the last sync counts as
    /// stale until the next one.
    ///
    /// # Errors
    ///
    /// [`StaleIdentity`] if the copy was never synced, or its last sync
    /// started more than `identity_max_staleness` ago.
    pub fn fresh(&self) -> Result<Arc<IdentitySnapshot>, StaleIdentity> {
        let snapshot = self.snapshot();
        let now = self.clock.now();
        match snapshot.synced_at.and_then(|at| now.checked_sub(at)) {
            Some(age) if age <= self.max_staleness => Ok(snapshot),
            _ => Err(StaleIdentity),
        }
    }

    /// Reads every identity register from `store` and replaces the copy.
    /// Registers that do not parse, and providers that share an issuer,
    /// are left out (see the [module documentation](self)).
    ///
    /// # Errors
    ///
    /// The control store's error if a register cannot be listed or read;
    /// the copy is then unchanged and keeps ageing.
    pub async fn sync<C: ControlStore>(
        &self,
        store: &C,
        retry: &RetryPolicy,
    ) -> Result<Arc<IdentitySnapshot>, ControlError> {
        self.sync_as_of(store, retry, self.clock.now()).await
    }

    /// As [`IdentityCopy::sync`], but the copy's age runs from `started`,
    /// time since the Unix epoch, instead of now. A restarted node that
    /// loads the copy it kept (§6.2) passes the start of the sync that
    /// produced it, so the copy does not look fresher than it is.
    ///
    /// # Errors
    ///
    /// As [`IdentityCopy::sync`].
    pub async fn sync_as_of<C: ControlStore>(
        &self,
        store: &C,
        retry: &RetryPolicy,
        started: Duration,
    ) -> Result<Arc<IdentitySnapshot>, ControlError> {
        let mut providers: BTreeMap<String, Vec<OidcProvider>> = BTreeMap::new();
        let mut roles = BTreeMap::new();
        for (key, _) in store.list(&KeyPrefix::identity()).await? {
            let name = |prefix: &str| {
                key.as_str()
                    .strip_prefix(prefix)
                    .and_then(|rest| rest.strip_suffix(JSON_SUFFIX))
                    .map(str::to_owned)
            };
            if name(PROVIDERS_PREFIX).is_some() {
                if let Some(document) =
                    read_valid::<ProviderDocument, _>(store, &key, retry).await?
                {
                    let provider = document.provider();
                    providers
                        .entry(provider.issuer.clone())
                        .or_default()
                        .push(provider);
                }
            } else if let Some(role) = name(ROLES_PREFIX) {
                if !RoleDocument::valid_name(&role) {
                    tracing::warn!(key = %key.as_str(), "ignoring a role with an invalid name");
                    continue;
                }
                if let Some(document) = read_valid::<RoleDocument, _>(store, &key, retry).await? {
                    let permissions = Permissions::new(
                        document
                            .policies
                            .iter()
                            .map(|policy| Arc::clone(policy.policy())),
                    );
                    let trust_policy = Arc::clone(document.trust_policy.policy());
                    roles.insert(
                        role,
                        Role {
                            trust_policy,
                            permissions,
                        },
                    );
                }
            } else {
                tracing::warn!(key = %key.as_str(), "ignoring an unknown identity register");
            }
        }
        let providers = providers
            .into_iter()
            .filter_map(|(issuer, mut listed)| {
                if listed.len() > 1 {
                    tracing::warn!(%issuer, "ignoring an issuer that several providers name");
                    return None;
                }
                listed.pop()
            })
            .collect();
        let snapshot = Arc::new(IdentitySnapshot {
            providers,
            roles,
            synced_at: Some(started),
        });
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&snapshot);
        Ok(snapshot)
    }
}

/// Reads the register at `key` as a `D`, or `None` if it is gone or does
/// not parse.
async fn read_valid<D: RegisterDocument, C: ControlStore>(
    store: &C,
    key: &RegisterKey,
    retry: &RetryPolicy,
) -> Result<Option<D>, ControlError> {
    match read_with_retries(store, &TypedKey::<D>::new(key.clone()), retry).await {
        Ok(document) => Ok(document.map(|document| document.value)),
        Err(ControlError::InvalidRegister { key, source }) => {
            tracing::warn!(key = %key.as_str(), error = %source, "ignoring an invalid identity register");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
