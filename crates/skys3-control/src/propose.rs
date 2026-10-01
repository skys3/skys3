//! Conditional writes with retries and the lost-response rule (design
//! §6.1), and typed register reads and writes.
//!
//! [`propose`] is the only way SkyS3 writes a register. It sends
//! [`ControlStore::put_if`] and handles every answer the same way for every
//! backend:
//!
//! - A `409`-style [`ControlError::Conflict`] or an
//!   [`ControlError::Unavailable`] store applied nothing: the same request
//!   is sent again after a backoff.
//! - A missing answer ([`ControlError::Indeterminate`]) may hide a write
//!   that landed, or one still in flight. The same value, with the same
//!   `proposal_id`, is sent again under the same precondition, so at most
//!   one of the attempts can be applied.
//! - A failed precondition after a missing answer triggers the
//!   lost-response rule: the register is re-read, and if it holds the
//!   proposal's own `proposal_id`, the write succeeded.
//!
//! An earlier attempt that landed and was then overwritten by another
//! writer is reported as [`ProposalOutcome::Rejected`]: the proposal is no
//! longer the register's value, and the caller acts on the value it reads
//! next, as it would after losing the race.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use skys3_types::{ProposalId, RegisterDocument};

use crate::key::{RegisterKey, TypedKey};
use crate::store::{ControlError, ControlStore, Expected, PutOutcome, Version, Versioned};

/// How [`propose`] retries a write that got no usable answer.
///
/// The backoff doubles from `initial_backoff` up to `max_backoff`, and each
/// wait is drawn from its upper half by hashing the proposal ID, so
/// proposers that collided once spread out without a shared random source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// How many requests, re-reads of the lost-response rule included, may
    /// go without a usable answer before giving up. At least 1.
    pub max_attempts: u32,
    /// The wait after the first failed attempt.
    pub initial_backoff: Duration,
    /// The longest wait between attempts.
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// The wait after failed attempt number `attempt` (from 1), jittered
    /// by `salt`.
    fn backoff(&self, attempt: u32, salt: &impl Hash) -> Duration {
        let doubling = 2_u32.saturating_pow(attempt.saturating_sub(1).min(20));
        let ceiling = self
            .initial_backoff
            .saturating_mul(doubling)
            .min(self.max_backoff);
        let mut hasher = DefaultHasher::new();
        (salt, attempt).hash(&mut hasher);
        let fraction = (hasher.finish() % 1024) as f64 / 1024.0;
        ceiling / 2 + (ceiling / 2).mul_f64(fraction)
    }
}

impl Default for RetryPolicy {
    /// Ten attempts, backing off from 20 ms to 1 s: about 4 s in all,
    /// several `409` races or a brief outage.
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_secs(1),
        }
    }
}

/// What a [`propose`] achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum ProposalOutcome {
    /// The register holds the proposal, at this version.
    Accepted(Version),
    /// Another writer won: the precondition failed, and the register does
    /// not hold the proposal.
    Rejected,
}

/// Writes `value`, which carries `proposal` as its `proposal_id`, if the
/// register is at `expected`, retrying under `policy` and applying the
/// lost-response rule.
///
/// # Errors
///
/// A non-retryable error from the store, or
/// [`ControlError::RetriesExhausted`] once `policy.max_attempts` requests
/// got no usable answer; its `may_have_applied` says whether the value may
/// still land.
pub async fn propose<S: ControlStore>(
    store: &S,
    key: &RegisterKey,
    expected: Expected,
    value: Bytes,
    proposal: &ProposalId,
    policy: &RetryPolicy,
) -> Result<ProposalOutcome, ControlError> {
    let mut uncertain = false;
    let mut check = false;
    let mut attempts = 0;
    // The last error, kept for the report when the budget runs out.
    let mut last: Option<ControlError> = None;
    let exhausted = |attempts, uncertain, last| ControlError::RetriesExhausted {
        key: key.clone(),
        attempts,
        may_have_applied: uncertain,
        last: Box::new(last),
    };
    loop {
        attempts += 1;
        let error = if check {
            match store.get(key).await {
                Ok(current) => return Ok(resolve(current, proposal)),
                Err(error) => error,
            }
        } else {
            match store.put_if(key, expected.clone(), value.clone()).await {
                Ok(PutOutcome::Written(version)) => return Ok(ProposalOutcome::Accepted(version)),
                Ok(PutOutcome::PreconditionFailed) if uncertain => {
                    // The re-read of the lost-response rule counts toward
                    // the budget like any other request.
                    if attempts >= policy.max_attempts {
                        let last = last.take().unwrap_or_else(|| {
                            ControlError::Indeterminate("an earlier write got no answer".into())
                        });
                        return Err(exhausted(attempts, uncertain, last));
                    }
                    check = true;
                    continue;
                }
                Ok(PutOutcome::PreconditionFailed) => return Ok(ProposalOutcome::Rejected),
                Err(error) => {
                    uncertain |= error.may_have_applied();
                    error
                }
            }
        };
        if !error.is_retryable() {
            return Err(error);
        }
        if attempts >= policy.max_attempts {
            return Err(exhausted(attempts, uncertain, error));
        }
        last = Some(error);
        tokio::time::sleep(policy.backoff(attempts, proposal)).await;
    }
}

/// Decides a proposal from the register's current value: accepted if the
/// value carries the proposal's ID.
fn resolve(current: Option<Versioned>, proposal: &ProposalId) -> ProposalOutcome {
    match current {
        Some(current) if proposal_id_of(&current.value).as_ref() == Some(proposal) => {
            ProposalOutcome::Accepted(current.version)
        }
        _ => ProposalOutcome::Rejected,
    }
}

/// Reads the `proposal_id` of any register value, whatever its document
/// type, or `None` if the value has none.
#[must_use]
pub fn proposal_id_of(value: &[u8]) -> Option<ProposalId> {
    #[derive(Deserialize)]
    struct Envelope {
        proposal_id: ProposalId,
    }
    serde_json::from_slice::<Envelope>(value)
        .ok()
        .map(|envelope| envelope.proposal_id)
}

/// Reads and validates a register document, or `None` if the register does
/// not exist.
///
/// # Errors
///
/// [`ControlError::InvalidRegister`] if the value is not a valid `D`, and
/// the store's errors.
pub async fn read<D: RegisterDocument, S: ControlStore>(
    store: &S,
    key: &TypedKey<D>,
) -> Result<Option<Versioned<D>>, ControlError> {
    let Some(current) = store.get(key.key()).await? else {
        return Ok(None);
    };
    let value = D::from_json(&current.value).map_err(|source| ControlError::InvalidRegister {
        key: key.key().clone(),
        source,
    })?;
    Ok(Some(Versioned {
        value,
        version: current.version,
    }))
}

/// [`read`]s a register document, retrying under `policy` while the store
/// does not answer.
///
/// # Errors
///
/// The errors of [`read`], and [`ControlError::RetriesExhausted`] once
/// `policy.max_attempts` reads got no answer.
pub async fn read_with_retries<D: RegisterDocument, S: ControlStore>(
    store: &S,
    key: &TypedKey<D>,
    policy: &RetryPolicy,
) -> Result<Option<Versioned<D>>, ControlError> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match read(store, key).await {
            Err(error) if error.is_retryable() => {
                if attempts >= policy.max_attempts {
                    return Err(ControlError::RetriesExhausted {
                        key: key.key().clone(),
                        attempts,
                        may_have_applied: false,
                        last: Box::new(error),
                    });
                }
                tokio::time::sleep(policy.backoff(attempts, key.key())).await;
            }
            result => return result,
        }
    }
}

/// Validates `document` and [`propose`]s it under its own proposal ID.
///
/// # Errors
///
/// [`ControlError::InvalidRegister`] if `document` breaks an invariant, and
/// the errors of [`propose`].
pub async fn propose_document<D: RegisterDocument, S: ControlStore>(
    store: &S,
    key: &TypedKey<D>,
    expected: Expected,
    document: &D,
    policy: &RetryPolicy,
) -> Result<ProposalOutcome, ControlError> {
    let value = document
        .to_json()
        .map_err(|source| ControlError::InvalidRegister {
            key: key.key().clone(),
            source,
        })?;
    propose(
        store,
        key.key(),
        expected,
        Bytes::from(value),
        document.proposal_id(),
        policy,
    )
    .await
}

/// A source of fresh proposal IDs: 128 random bits each.
#[derive(Debug, Clone)]
pub struct ProposalIds {
    rng: SmallRng,
}

impl ProposalIds {
    /// A source seeded from the operating system, for nodes.
    #[must_use]
    pub fn from_os_rng() -> Self {
        Self {
            rng: SmallRng::from_os_rng(),
        }
    }

    /// A source seeded from `seed`, for simulation and tests.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// The next proposal ID.
    pub fn next_id(&mut self) -> ProposalId {
        ProposalId::from_u128(self.rng.random())
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::CoordinatorLease;

    use super::*;
    use crate::faults::{Fault, FaultyStore};
    use crate::memory::MemoryControlStore;

    fn lease(holder: &str, proposal: &ProposalId) -> CoordinatorLease {
        CoordinatorLease {
            holder: holder.parse().unwrap(),
            proposal_id: proposal.clone(),
        }
    }

    fn quick() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            ..RetryPolicy::default()
        }
    }

    #[test]
    fn backoff_doubles_within_bounds() {
        let policy = RetryPolicy::default();
        let id = ProposalId::from_u128(7);
        let waits: Vec<_> = (1..=12).map(|n| policy.backoff(n, &id)).collect();
        for (n, wait) in waits.iter().enumerate() {
            let ceiling = (policy.initial_backoff * 2_u32.pow(n as u32)).min(policy.max_backoff);
            assert!(*wait >= ceiling / 2 && *wait <= ceiling, "{n}: {wait:?}");
        }
        assert_eq!(policy.backoff(3, &id), policy.backoff(3, &id));
        assert!(policy.backoff(u32::MAX, &id) <= policy.max_backoff);
    }

    #[test]
    fn proposal_ids_are_fresh_and_seedable() {
        let mut a = ProposalIds::seeded(1);
        let mut b = ProposalIds::seeded(1);
        let first = a.next_id();
        assert_eq!(first, b.next_id());
        assert_ne!(first, a.next_id());
        assert_ne!(ProposalIds::from_os_rng().next_id(), first);
    }

    #[test]
    fn proposal_ids_are_found_in_any_register_value() {
        let id = ProposalId::new("p-1").unwrap();
        let value = lease("node-1", &id).to_json().unwrap();
        assert_eq!(proposal_id_of(&value), Some(id));
        assert_eq!(
            proposal_id_of(br#"{"x":1,"proposal_id":"q"}"#)
                .unwrap()
                .as_str(),
            "q"
        );
        assert_eq!(proposal_id_of(br#"{"proposal_id":"a.b"}"#), None);
        assert_eq!(proposal_id_of(b"not json"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_response_of_an_applied_write_is_accepted() {
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script([Fault::LoseResponse]);
        let key = TypedKey::coordinator_lease();
        let id = ProposalId::new("mine").unwrap();
        let outcome = propose_document(
            &store,
            &key,
            Expected::Absent,
            &lease("node-1", &id),
            &quick(),
        )
        .await
        .unwrap();
        let stored = read(&store, &key).await.unwrap().unwrap();
        assert_eq!(outcome, ProposalOutcome::Accepted(stored.version));
        assert_eq!(stored.value.proposal_id, id);
        // Put (lost), put (412), and the re-read.
        assert_eq!(store.requests(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_request_is_sent_again() {
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script([Fault::LoseRequest, Fault::Conflict, Fault::Unavailable]);
        let key = TypedKey::coordinator_lease();
        let id = ProposalId::new("mine").unwrap();
        let outcome = propose_document(
            &store,
            &key,
            Expected::Absent,
            &lease("node-1", &id),
            &quick(),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
        assert_eq!(
            read(&store, &key).await.unwrap().unwrap().value.proposal_id,
            id
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_write_overwritten_by_another_writer_is_rejected() {
        let memory = MemoryControlStore::new();
        let store = FaultyStore::new(memory.clone());
        let key = TypedKey::coordinator_lease();
        let theirs = ProposalId::new("theirs").unwrap();
        let mine = ProposalId::new("mine").unwrap();
        // Our write lands, its answer is lost, and another writer then
        // replaces it before we retry.
        let other = memory.clone();
        let their_value = lease("node-2", &theirs);
        store.script([Fault::lose_response_after(move || {
            let (other, their_value) = (other.clone(), their_value.clone());
            async move {
                let key = RegisterKey::coordinator_lease();
                let current = other.get(&key).await.unwrap();
                let expected = Expected::Version(current.unwrap().version);
                let value = Bytes::from(their_value.to_json().unwrap());
                let written = other.put_if(&key, expected, value);
                assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
            }
        })]);
        let outcome = propose_document(
            &store,
            &key,
            Expected::Absent,
            &lease("node-1", &mine),
            &quick(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ProposalOutcome::Rejected);
        assert_eq!(
            read(&store, &key).await.unwrap().unwrap().value.proposal_id,
            theirs
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_plain_precondition_failure_is_rejected_without_a_read() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let key = TypedKey::coordinator_lease();
        let first = ProposalId::new("first").unwrap();
        let second = ProposalId::new("second").unwrap();
        let policy = quick();
        let doc = lease("node-1", &first);
        assert!(matches!(
            propose_document(&store, &key, Expected::Absent, &doc, &policy).await,
            Ok(ProposalOutcome::Accepted(_))
        ));
        let doc = lease("node-2", &second);
        let outcome = propose_document(&store, &key, Expected::Absent, &doc, &policy).await;
        assert_eq!(outcome.unwrap(), ProposalOutcome::Rejected);
        assert_eq!(store.requests(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn attempts_are_bounded_and_report_uncertainty() {
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script(vec![Fault::LoseRequest; 4]);
        let key = TypedKey::coordinator_lease();
        let doc = lease("node-1", &ProposalId::new("p").unwrap());
        let error = propose_document(&store, &key, Expected::Absent, &doc, &quick())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ControlError::RetriesExhausted {
                attempts: 4,
                may_have_applied: true,
                ..
            }
        ));
        store.script(vec![Fault::Unavailable; 4]);
        let error = propose_document(&store, &key, Expected::Absent, &doc, &quick())
            .await
            .unwrap_err();
        assert!(!error.may_have_applied(), "{error}");
        // A failing re-read is retried too.
        store.script([
            Fault::LoseResponse,
            Fault::Unavailable,
            Fault::Unavailable,
            Fault::Unavailable,
        ]);
        let error = propose_document(&store, &key, Expected::Absent, &doc, &quick())
            .await
            .unwrap_err();
        assert!(error.may_have_applied(), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_lost_response_re_read_counts_toward_the_budget() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let key = TypedKey::coordinator_lease();
        let doc = lease("node-1", &ProposalId::new("p").unwrap());
        let policy = RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        };
        // The first write lands with its answer lost; the second fails its
        // precondition on the last permitted request, so no re-read is sent.
        store.script([Fault::LoseResponse]);
        let error = propose_document(&store, &key, Expected::Absent, &doc, &policy)
            .await
            .unwrap_err();
        assert_eq!(store.requests(), 2);
        let ControlError::RetriesExhausted {
            attempts,
            may_have_applied,
            last,
            ..
        } = error
        else {
            panic!("{error}");
        };
        assert_eq!(attempts, 2);
        assert!(may_have_applied);
        assert!(matches!(*last, ControlError::Indeterminate(_)), "{last}");
        // With one more request allowed, the re-read resolves it.
        let policy = RetryPolicy {
            max_attempts: 3,
            ..policy
        };
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script([Fault::LoseResponse]);
        let outcome = propose_document(&store, &key, Expected::Absent, &doc, &policy).await;
        assert!(
            matches!(outcome, Ok(ProposalOutcome::Accepted(_))),
            "{outcome:?}"
        );
        assert_eq!(store.requests(), 3);
    }

    #[tokio::test]
    async fn invalid_documents_are_neither_written_nor_read() {
        let store = MemoryControlStore::new();
        let key = TypedKey::cluster();
        let document = skys3_types::ClusterDocument {
            cluster_id: "c".parse().unwrap(),
            format_version: 99,
            generation: skys3_types::Generation::new(1),
            proposal_id: ProposalId::new("p").unwrap(),
        };
        let error = propose_document(&store, &key, Expected::Absent, &document, &quick())
            .await
            .unwrap_err();
        assert!(
            matches!(error, ControlError::InvalidRegister { .. }),
            "{error}"
        );
        let written = store
            .put_if(key.key(), Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        assert!(matches!(written, PutOutcome::Written(_)));
        let error = read(&store, &key).await.unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("register cluster.json is invalid"),
            "{error}"
        );
        assert!(
            read(&store, &TypedKey::coordinator_lease())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn non_retryable_errors_end_the_proposal() {
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script([Fault::Fail]);
        let doc = lease("node-1", &ProposalId::new("p").unwrap());
        let error = propose_document(
            &store,
            &TypedKey::coordinator_lease(),
            Expected::Absent,
            &doc,
            &quick(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
    }
}
