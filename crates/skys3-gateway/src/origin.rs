//! What a gateway knows of the origins of `read_only` buckets (design
//! §9.5).
//!
//! A `read_only` bucket caches the objects of an origin that SkyS3 does not
//! own, which anyone may change at any time. A read is served from the
//! cache only for the version the origin was seen to hold: under
//! `freshness = "revalidate"`, a HEAD of the origin made for that read;
//! under `freshness = "ttl"`, a HEAD made at most `freshness_ttl_seconds`
//! before it. The gateway keeps what its HEADs saw as
//! [`OriginValidations`], keyed by the [`OriginScope`] and the key: the
//! origin and the credentials it was read with. Two buckets with the same
//! origin and credentials share what they learn of it; buckets whose
//! credentials differ never do, since the origin may show their scopes
//! different objects, or refuse one of them.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_types::{ETag, RemoteTarget};
use tokio::time::Instant;

use crate::remote::RemoteObject;

/// The most keys whose validation a gateway remembers; the oldest are
/// forgotten first, and a forgotten key is revalidated by its next read.
pub const MAX_ORIGIN_VALIDATIONS: usize = 65_536;

/// The origin of a `read_only` bucket and the scope of the credentials a
/// node reads it with: what the gateway's knowledge of the origin's
/// objects is keyed by, beside the key (§9.5).
///
/// ```
/// use skys3_gateway::OriginScope;
/// use skys3_types::RemoteTarget;
///
/// let target = RemoteTarget {
///     endpoint: "https://s3.us-east-1.amazonaws.com".to_owned(),
///     bucket: "datasets".to_owned(),
///     prefix: Some("public/".to_owned()),
/// };
/// let reader = OriginScope::new(&target, "profile:reader");
/// assert_ne!(reader, OriginScope::new(&target, "default"));
/// assert_eq!(reader.credentials(), "profile:reader");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginScope {
    endpoint: String,
    bucket: String,
    prefix: String,
    credentials: String,
}

impl OriginScope {
    /// The scope of `target` read with the credentials named
    /// `credentials`, such as `default` or `profile:<name>`.
    #[must_use]
    pub fn new(target: &RemoteTarget, credentials: impl Into<String>) -> Self {
        Self {
            endpoint: target.endpoint.clone(),
            bucket: target.bucket.clone(),
            prefix: target.prefix.clone().unwrap_or_default(),
            credentials: credentials.into(),
        }
    }

    /// The scope of the credentials.
    #[must_use]
    pub fn credentials(&self) -> &str {
        &self.credentials
    }
}

impl fmt::Display for OriginScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{} as {}",
            self.endpoint, self.bucket, self.prefix, self.credentials
        )
    }
}

/// What a HEAD of a key at a `read_only` bucket's origin found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginHead {
    /// The object the origin holds.
    Found(RemoteObject),
    /// The origin has no object at the key.
    Missing,
    /// The origin refused the bucket's credentials the object: `403`.
    Denied,
}

/// What the origin held at a key when a gateway last asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Seen {
    /// An object with this ETag.
    Present(ETag),
    /// No object.
    Missing,
    /// An object the credentials may not read.
    Denied,
}

impl From<&OriginHead> for Seen {
    fn from(head: &OriginHead) -> Self {
        match head {
            OriginHead::Found(found) => Self::Present(found.object.local_etag.clone()),
            OriginHead::Missing => Self::Missing,
            OriginHead::Denied => Self::Denied,
        }
    }
}

type Name = (Arc<OriginScope>, String);

/// What a gateway's HEADs saw at the origins of `read_only` buckets, and
/// when each was sent (§9.5): under `freshness = "ttl"`, a read whose key
/// was seen within the TTL is served from what was seen, without asking
/// the origin again.
///
/// Each answer is kept from the moment its HEAD was sent, so whatever the
/// origin held when it answered is at most that old. Answers are held in
/// memory, at most [`MAX_ORIGIN_VALIDATIONS`] of them; a gateway that
/// restarts asks again. Clones share them.
#[derive(Clone, Default)]
pub struct OriginValidations {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    /// A seeded bug for the simulation, which only `test-util` builds can
    /// set: answers are keyed by the origin alone, without the scope of
    /// the credentials.
    ignore_credentials: AtomicBool,
    /// A seeded bug for the simulation, which only `test-util` builds can
    /// set: a read under `revalidate` serves a cached copy without asking
    /// the origin.
    skip_revalidation: AtomicBool,
}

#[derive(Default)]
struct State {
    seen: BTreeMap<Name, (Instant, Seen)>,
    /// The names in the order they were seen, with when, to forget the
    /// oldest first; a name seen again is in it more than once, and only
    /// its latest time counts.
    order: VecDeque<(Instant, Name)>,
}

impl fmt::Debug for OriginValidations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginValidations")
            .field("keys", &self.lock().seen.len())
            .finish_non_exhaustive()
    }
}

impl OriginValidations {
    /// Validations that remember nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds a bug for the simulation: answers are keyed by the origin
    /// alone, so a bucket is served what another's credentials saw.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn ignore_credentials(&self) {
        self.inner.ignore_credentials.store(true, Ordering::Relaxed);
    }

    /// Seeds a bug for the simulation: under `revalidate`, a read is
    /// served from a cached copy without asking the origin.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn skip_revalidation(&self) {
        self.inner.skip_revalidation.store(true, Ordering::Relaxed);
    }

    /// Whether the seeded bug of [`OriginValidations::skip_revalidation`]
    /// is set.
    pub(crate) fn skips_revalidation(&self) -> bool {
        self.inner.skip_revalidation.load(Ordering::Relaxed)
    }

    /// The number of keys whose answers are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().seen.len()
    }

    /// Whether no answer is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// What the origin held at `key`, as a HEAD sent less than `ttl` ago
    /// saw it, if one was.
    pub(crate) fn fresh(&self, scope: &OriginScope, key: &str, ttl: Duration) -> Option<Seen> {
        let name = self.name(scope, key);
        let state = self.lock();
        let (at, seen) = state.seen.get(&name)?;
        (at.elapsed() < ttl).then(|| seen.clone())
    }

    /// Records what a HEAD of `key` sent at `at` saw, unless a later HEAD's
    /// answer is already recorded.
    pub(crate) fn record(&self, scope: &OriginScope, key: &str, at: Instant, seen: Seen) {
        let name = self.name(scope, key);
        let mut state = self.lock();
        if state
            .seen
            .get(&name)
            .is_some_and(|(recorded, _)| *recorded > at)
        {
            return;
        }
        state.seen.insert(name.clone(), (at, seen));
        state.order.push_back((at, name));
        state.trim();
    }

    /// Forgets what was seen of `key` at `scope`, which turned out to have
    /// changed since: the next read asks the origin.
    pub(crate) fn forget(&self, scope: &OriginScope, key: &str) {
        let name = self.name(scope, key);
        self.lock().seen.remove(&name);
    }

    /// The name answers for `key` at `scope` are kept under.
    fn name(&self, scope: &OriginScope, key: &str) -> Name {
        let scope = if self.inner.ignore_credentials.load(Ordering::Relaxed) {
            OriginScope {
                credentials: String::new(),
                ..scope.clone()
            }
        } else {
            scope.clone()
        };
        (Arc::new(scope), key.to_owned())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Plain inserts and removals, which a panic cannot leave half done.
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// Forgets the oldest answers beyond [`MAX_ORIGIN_VALIDATIONS`], and
    /// drops the order's stale entries once they outnumber the live ones.
    fn trim(&mut self) {
        while self.seen.len() > MAX_ORIGIN_VALIDATIONS {
            let Some((at, name)) = self.order.pop_front() else {
                break;
            };
            if self.seen.get(&name).is_some_and(|(seen, _)| *seen == at) {
                self.seen.remove(&name);
            }
        }
        if self.order.len() > 2 * self.seen.len().max(MAX_ORIGIN_VALIDATIONS / 16) {
            let seen = &self.seen;
            self.order
                .retain(|(at, name)| seen.get(name).is_some_and(|(latest, _)| latest == at));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(credentials: &str) -> OriginScope {
        let target = RemoteTarget {
            endpoint: "https://origin.example".to_owned(),
            bucket: "data".to_owned(),
            prefix: None,
        };
        OriginScope::new(&target, credentials)
    }

    fn etag(n: u8) -> ETag {
        ETag::new(format!("{n:032x}")).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn answers_are_fresh_for_the_ttl_from_their_head() {
        let validations = OriginValidations::new();
        let (scope, ttl) = (scope("default"), Duration::from_secs(10));
        let sent = Instant::now();
        tokio::time::advance(Duration::from_secs(2)).await;
        validations.record(&scope, "k", sent, Seen::Present(etag(1)));
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(
            validations.fresh(&scope, "k", ttl),
            Some(Seen::Present(etag(1)))
        );
        // Counted from when the HEAD was sent, not answered.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(validations.fresh(&scope, "k", ttl), None);
        assert_eq!(validations.fresh(&scope, "other", ttl), None);
    }

    #[tokio::test(start_paused = true)]
    async fn an_earlier_head_never_replaces_a_later_one() {
        let validations = OriginValidations::new();
        let scope = scope("default");
        let early = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        validations.record(&scope, "k", Instant::now(), Seen::Missing);
        validations.record(&scope, "k", early, Seen::Present(etag(1)));
        let fresh = validations.fresh(&scope, "k", Duration::from_secs(5));
        assert_eq!(fresh, Some(Seen::Missing));
        validations.forget(&scope, "k");
        assert_eq!(validations.fresh(&scope, "k", Duration::from_secs(5)), None);
    }

    #[tokio::test(start_paused = true)]
    async fn credentials_scope_what_is_known() {
        let validations = OriginValidations::new();
        let ttl = Duration::from_secs(5);
        validations.record(&scope("profile:a"), "k", Instant::now(), Seen::Denied);
        assert_eq!(
            validations.fresh(&scope("profile:a"), "k", ttl),
            Some(Seen::Denied)
        );
        assert_eq!(validations.fresh(&scope("profile:b"), "k", ttl), None);
        assert_eq!(validations.len(), 1);
        assert!(!validations.is_empty());
        assert!(format!("{validations:?}").contains("keys: 1"));
        assert_eq!(
            scope("profile:a").to_string(),
            "https://origin.example/data/ as profile:a"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_oldest_answers_are_forgotten_first() {
        let validations = OriginValidations::new();
        let scope = scope("default");
        let ttl = Duration::from_secs(3600);
        // The first key is seen again, after every other.
        for n in 0..MAX_ORIGIN_VALIDATIONS + 2 {
            validations.record(&scope, &format!("k{n}"), Instant::now(), Seen::Missing);
            tokio::time::advance(Duration::from_millis(1)).await;
            if n == MAX_ORIGIN_VALIDATIONS {
                validations.record(&scope, "k0", Instant::now(), Seen::Missing);
            }
        }
        assert_eq!(validations.len(), MAX_ORIGIN_VALIDATIONS);
        assert!(validations.fresh(&scope, "k0", ttl).is_some());
        assert!(validations.fresh(&scope, "k1", ttl).is_none());
        assert!(validations.fresh(&scope, "k2", ttl).is_none());
        assert!(validations.fresh(&scope, "k3", ttl).is_some());
        let state = validations.lock();
        assert!(state.order.len() <= 2 * MAX_ORIGIN_VALIDATIONS);
    }
}
