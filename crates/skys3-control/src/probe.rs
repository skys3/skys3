//! The startup probe of a control store (design §6.1).

use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinSet;

use crate::key::{KeyPrefix, RegisterKey};
use crate::propose::{
    DeletionOutcome, ProposalIds, ProposalOutcome, RetryPolicy, get_with_retries, propose,
    propose_delete,
};
use crate::store::{ControlError, ControlStore, Expected, Version, Versioned};

/// The startup probe of a control store.
///
/// SkyS3's correctness rests on two properties of the control store:
/// conditional writes to one register are linearizable, and a read after a
/// write sees it, which the lost-response rule needs. An S3-compatible
/// store may lack either, so each node runs [`ControlProbe`] before it uses
/// one and refuses the store if any round fails. The probe is generic over
/// [`ControlStore`], so the conformance suite (plan M2-06) runs the same
/// probe against every backend.
///
/// # Rounds
///
/// The probe runs [`ControlProbe::ROUNDS`] rounds across
/// [`ControlProbe::KEYS`] scratch registers,
/// `probe/<nonce>/<n>.json`, where the nonce is fresh for every run so
/// that probes from several nodes never share registers. In each round,
/// one writer per store handle the caller passes (at least two) proposes a
/// value of its own for the round's register, all under the same
/// precondition: `If-None-Match: *` for the register's first round, and
/// `If-Match` on the previous winner's version after that. Writers race
/// as concurrent requests and go through [`propose`], the path every
/// register write takes, so `409` conflicts are retried and lost responses
/// are resolved as in production. A round passes when:
///
/// - exactly one writer's proposal is accepted,
/// - every other writer, reading the register right after it lost, reads
///   the winner's value at the winner's version, and
/// - a listing of the scratch registers right after the round shows every
///   register written so far at its latest version, and nothing else.
///
/// The listing check is there because change streams list the registers
/// once per generation they observe (design §6.2): a listing that lags the
/// writes a generation announces would lose those writes until the next
/// increment.
///
/// # Deletes
///
/// Registers are deleted with `delete_if`, which S3 stores implement with
/// `If-Match` on `DeleteObject`, a precondition some providers do not
/// honor. After the rounds, the probe deletes each scratch register once
/// at a version it no longer has, which must be refused, then at its
/// current version, which must succeed, and then reads it, which must find
/// nothing, and a listing must no longer show it. A store that ignores or
/// rejects the precondition is refused like one that fails a round
/// (design §6.1 says why there is no fallback).
///
/// # Cleanup
///
/// Whether or not it passes, the probe deletes every scratch register it
/// may have written, at its current version, and reports the ones it could
/// not delete in [`ProbeError::leftovers`]. A write still in flight after
/// its request timed out may create a scratch register after the cleanup;
/// change streams never report registers under `probe/`. On a versioned
/// control bucket the probe's writes stay behind as noncurrent versions,
/// like every overwritten register's.
///
/// # Example
///
/// ```
/// use skys3_control::{ControlProbe, MemoryControlStore};
///
/// # tokio::runtime::Builder::new_current_thread().enable_time().build()?.block_on(async {
/// let probe = ControlProbe::new(0x2a);
/// assert_eq!(probe.scratch_prefix().as_str(), "probe/000000000000002a/");
/// let store = MemoryControlStore::new();
/// probe.run(&[store.clone(), store.clone(), store.clone()]).await?;
/// assert!(store.registers().is_empty());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProbe {
    nonce: u64,
    rounds: u32,
    keys: u32,
    policy: RetryPolicy,
}

impl ControlProbe {
    /// The rounds a probe runs.
    pub const ROUNDS: u32 = 100;

    /// The scratch registers the rounds cycle through.
    pub const KEYS: u32 = 4;

    /// A probe with `nonce` in its scratch keys, [`Self::ROUNDS`] rounds
    /// over [`Self::KEYS`] registers, and the default retry policy.
    /// Simulations pass a nonce derived from their seed; nodes use
    /// [`ControlProbe::with_fresh_nonce`].
    #[must_use]
    pub fn new(nonce: u64) -> Self {
        Self {
            nonce,
            rounds: Self::ROUNDS,
            keys: Self::KEYS,
            policy: RetryPolicy::default(),
        }
    }

    /// A probe with a random nonce from the operating system.
    #[must_use]
    pub fn with_fresh_nonce() -> Self {
        Self::new(SmallRng::from_os_rng().random())
    }

    /// Runs `rounds` rounds over `keys` registers instead, for tests and
    /// for the conformance suite's runs at scale. Both are at least 1.
    #[must_use]
    pub fn with_rounds(mut self, rounds: u32, keys: u32) -> Self {
        self.rounds = rounds.max(1);
        self.keys = keys.max(1);
        self
    }

    /// Retries every request under `policy` instead of the default.
    #[must_use]
    pub fn with_policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The prefix of every scratch register, `probe/<nonce>/`.
    #[must_use]
    pub fn scratch_prefix(&self) -> KeyPrefix {
        KeyPrefix::new(format!("probe/{:016x}/", self.nonce))
            .expect("hex digits form a valid prefix")
    }

    /// Scratch register `n`.
    fn key(&self, n: u32) -> RegisterKey {
        RegisterKey::new(format!("{}{n}.json", self.scratch_prefix()))
            .expect("a probe key is a valid register key")
    }

    /// Probes the store behind `writers`, one racing writer per handle,
    /// and removes the scratch registers.
    ///
    /// # Errors
    ///
    /// A [`ProbeError`] naming the step that failed, and what the cleanup
    /// left behind. [`ProbeError::refuses_store`] says whether the store
    /// broke a property, or only failed to answer, so that probing again
    /// later may pass.
    ///
    /// # Panics
    ///
    /// If `writers` has fewer than two handles: a race needs two writers.
    pub async fn run<S: ControlStore>(&self, writers: &[S]) -> Result<(), ProbeError> {
        assert!(writers.len() >= 2, "the probe races at least two writers");
        let mut run = Run {
            probe: self,
            writers,
            ids: ProposalIds::seeded(self.nonce),
            registers: BTreeMap::new(),
        };
        let result = run.probe().await;
        let cleanup = run.clean_up().await;
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err((error, leftovers))) => Err(ProbeError {
                step: "cleanup".to_owned(),
                failure: ProbeFailure::Store(error),
                leftovers,
            }),
            (Err((step, failure)), cleanup) => Err(ProbeError {
                step,
                failure,
                leftovers: cleanup
                    .err()
                    .map(|(_, leftovers)| leftovers)
                    .unwrap_or_default(),
            }),
        }
    }
}

/// What broke a probe.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProbeFailure {
    /// A round had no winner or several: conditional writes are ignored,
    /// or a precondition that held failed.
    #[error("{0} racing conditional writers won, not exactly one")]
    Winners(usize),
    /// A writer that lost read something other than the winner's value.
    #[error("a writer that lost read {found}, not the winner's value")]
    StaleRead {
        /// What it read.
        found: String,
    },
    /// A delete at a version the register no longer had was applied.
    #[error("a conditional delete at a stale version was applied")]
    DeleteIgnored,
    /// A delete at the register's current version was refused.
    #[error("a conditional delete at the current version was refused")]
    DeleteRefused,
    /// A listing missed a register written before it, showed an old
    /// version, or showed a deleted register.
    #[error("a listing showed {found}")]
    StaleListing {
        /// What the listing got wrong.
        found: String,
    },
    /// A register was read after it was deleted.
    #[error("a deleted register was still read")]
    DeletedStillRead,
    /// The store failed a request.
    #[error(transparent)]
    Store(#[from] ControlError),
}

impl ProbeFailure {
    /// Whether the store broke a property it must have. A store that only
    /// failed to answer may pass if probed again.
    #[must_use]
    pub fn refuses_store(&self) -> bool {
        match self {
            Self::Store(error) => {
                !error.is_retryable() && !matches!(error, ControlError::RetriesExhausted { .. })
            }
            _ => true,
        }
    }
}

/// A failed probe: the step, what failed, and what the cleanup could not
/// remove.
#[derive(Debug, thiserror::Error)]
#[error("control-store probe failed at {step}: {failure}{}", Leftovers(&self.leftovers))]
pub struct ProbeError {
    /// The step that failed, such as `round 17 on probe/<nonce>/1.json`,
    /// or `cleanup` if only the cleanup failed.
    pub step: String,
    /// What failed.
    pub failure: ProbeFailure,
    /// The scratch registers the cleanup could not delete.
    pub leftovers: Vec<RegisterKey>,
}

impl ProbeError {
    /// Whether the store is refused: it broke a property, rather than
    /// failing to answer (see [`ProbeFailure::refuses_store`]).
    #[must_use]
    pub fn refuses_store(&self) -> bool {
        self.failure.refuses_store()
    }
}

/// Formats a non-empty leftover list as `; left behind: a, b`.
struct Leftovers<'a>(&'a [RegisterKey]);

impl fmt::Display for Leftovers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, key) in self.0.iter().enumerate() {
            let separator = if i == 0 { "; left behind: " } else { ", " };
            write!(f, "{separator}{key}")?;
        }
        Ok(())
    }
}

/// A failed step: its description, and what failed.
type StepError = (String, ProbeFailure);

/// One writer's part in a round: its value, its outcome, and, if it lost,
/// what it read next.
type Attempt = (Bytes, ProposalOutcome, Option<Option<Versioned>>);

/// One run of the probe.
struct Run<'a, S> {
    probe: &'a ControlProbe,
    writers: &'a [S],
    ids: ProposalIds,
    /// Each scratch register written: its current version, and the one it
    /// had before, if any.
    registers: BTreeMap<RegisterKey, (Version, Option<Version>)>,
}

impl<S: ControlStore> Run<'_, S> {
    async fn probe(&mut self) -> Result<(), StepError> {
        for round in 0..self.probe.rounds {
            let key = self.probe.key(round % self.probe.keys);
            let lister = &self.writers[round as usize % self.writers.len()];
            let checked = async {
                self.round(round, &key).await?;
                self.check_listing(lister).await
            };
            checked
                .await
                .map_err(|failure| (format!("round {round} on {key}"), failure))?;
        }
        let keys: Vec<RegisterKey> = self.registers.keys().cloned().collect();
        for key in keys {
            let checked = async {
                let (current, previous) = self.registers[&key].clone();
                self.delete(&key, current, previous).await?;
                self.registers.remove(&key);
                self.check_listing(&self.writers[1]).await
            };
            checked
                .await
                .map_err(|failure| (format!("the deletion of {key}"), failure))?;
        }
        Ok(())
    }

    /// Lists the scratch registers through `lister`, which must show
    /// exactly the registers written so far, each at its latest version.
    async fn check_listing(&self, lister: &S) -> Result<(), ProbeFailure> {
        let prefix = self.probe.scratch_prefix();
        let policy = &self.probe.policy;
        let mut attempts = 0;
        let listed = loop {
            attempts += 1;
            match lister.list(&prefix).await {
                Ok(listed) => break listed,
                Err(error) if error.is_retryable() && attempts < policy.max_attempts => {
                    tokio::time::sleep(policy.initial_backoff).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let listed: BTreeMap<RegisterKey, Version> = listed.into_iter().collect();
        let written = self
            .registers
            .iter()
            .map(|(key, (version, _))| (key, version));
        for (key, version) in written {
            match listed.get(key) {
                Some(found) if found == version => {}
                Some(found) => {
                    return Err(ProbeFailure::StaleListing {
                        found: format!("{key} at version {found}, not {version}"),
                    });
                }
                None => {
                    return Err(ProbeFailure::StaleListing {
                        found: format!("no {key}"),
                    });
                }
            }
        }
        match listed.keys().find(|key| !self.registers.contains_key(*key)) {
            Some(key) => Err(ProbeFailure::StaleListing {
                found: format!("{key}, which is deleted or was never written"),
            }),
            None => Ok(()),
        }
    }

    /// One round: every writer proposes, exactly one wins, and the losers
    /// read the winner's value.
    async fn round(&mut self, round: u32, key: &RegisterKey) -> Result<(), ProbeFailure> {
        let expected = match self.registers.get(key) {
            Some((version, _)) => Expected::Version(version.clone()),
            None => Expected::Absent,
        };
        let mut tasks = JoinSet::new();
        for (writer, store) in self.writers.iter().enumerate() {
            let proposal = self.ids.next_id();
            let value = Bytes::from(format!(
                r#"{{"proposal_id":"{proposal}","round":{round},"writer":{writer}}}"#
            ));
            let (store, key, expected) = (store.clone(), key.clone(), expected.clone());
            let policy = self.probe.policy;
            tasks.spawn(async move {
                let outcome =
                    propose(&store, &key, expected, value.clone(), &proposal, &policy).await?;
                let read = match outcome {
                    ProposalOutcome::Accepted(_) => None,
                    ProposalOutcome::Rejected => {
                        Some(get_with_retries(&store, &key, &policy).await?)
                    }
                };
                Ok::<Attempt, ControlError>((value, outcome, read))
            });
        }
        let attempts: Vec<Attempt> = tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let winners: Vec<(&Bytes, &Version)> = attempts
            .iter()
            .filter_map(|(value, outcome, _)| match outcome {
                ProposalOutcome::Accepted(version) => Some((value, version)),
                ProposalOutcome::Rejected => None,
            })
            .collect();
        let [(value, version)] = winners[..] else {
            return Err(ProbeFailure::Winners(winners.len()));
        };
        // Recorded before the checks, so the cleanup knows the version.
        let previous = self.registers.get(key).map(|(current, _)| current.clone());
        self.registers
            .insert(key.clone(), (version.clone(), previous));
        for read in attempts.iter().filter_map(|(_, _, read)| read.as_ref()) {
            match read {
                Some(read) if read.value == value && read.version == *version => {}
                Some(read) => {
                    return Err(ProbeFailure::StaleRead {
                        found: format!("version {}", read.version),
                    });
                }
                None => {
                    return Err(ProbeFailure::StaleRead {
                        found: "nothing".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Deletes a register at a stale version, which must be refused, then
    /// at its current one, and reads it from another writer.
    async fn delete(
        &self,
        key: &RegisterKey,
        current: Version,
        previous: Option<Version>,
    ) -> Result<(), ProbeFailure> {
        let policy = &self.probe.policy;
        let (deleter, reader) = (&self.writers[0], &self.writers[1]);
        // A register with one round has no stale version: use one that no
        // store assigns.
        let stale = previous.unwrap_or_else(|| Version::new("skys3-probe-mismatch"));
        if propose_delete(deleter, key, &stale, policy).await? == DeletionOutcome::Deleted {
            return Err(ProbeFailure::DeleteIgnored);
        }
        if propose_delete(deleter, key, &current, policy).await? == DeletionOutcome::Rejected {
            return Err(ProbeFailure::DeleteRefused);
        }
        if get_with_retries(reader, key, policy).await?.is_some() {
            return Err(ProbeFailure::DeletedStillRead);
        }
        Ok(())
    }

    /// Deletes every scratch register the run may have written, at its
    /// current version, and returns the first error and the registers left.
    async fn clean_up(&mut self) -> Result<(), (ControlError, Vec<RegisterKey>)> {
        let store = &self.writers[0];
        let policy = &self.probe.policy;
        let mut first_error = None;
        let mut leftovers = Vec::new();
        for n in 0..self.probe.keys.min(self.probe.rounds) {
            let key = self.probe.key(n);
            let known = self.registers.get(&key).map(|(version, _)| version);
            // A store with stale reads may show a deleted value again, or
            // a present one as absent, so the version the probe knows is
            // deleted first, and the deletes are bounded.
            let removed = async {
                if let Some(version) = known {
                    propose_delete(store, &key, version, policy)
                        .await
                        .map(drop)?;
                }
                for _ in 0..policy.max_attempts {
                    match get_with_retries(store, &key, policy).await? {
                        Some(current) => propose_delete(store, &key, &current.version, policy)
                            .await
                            .map(drop)?,
                        None => return Ok(()),
                    }
                }
                Err(ControlError::Unavailable(format!(
                    "{key} was still read after {} deletes",
                    policy.max_attempts
                )))
            };
            if let Err(error) = removed.await {
                leftovers.push(key);
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            None => Ok(()),
            Some(error) => Err((error, leftovers)),
        }
    }
}
