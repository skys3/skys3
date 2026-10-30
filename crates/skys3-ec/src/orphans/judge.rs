//! A shard primary's verdicts on its fragment nodes' orphan queries.

use std::collections::BTreeMap;
use std::sync::Arc;

use skys3_index::{Entry, Index, IndexError};
use skys3_io::{BlockingPool, Disk};
use skys3_log::ShardRef;
use skys3_shard::{Role, Shard};
use skys3_types::NodeId;

use super::{Suspect, Verdict};
use crate::encoder::Attempts;

/// Why a replica gives no verdicts.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum JudgeError {
    /// The replica does not lead the shard, or does not serve yet: it has
    /// not reconciled the members since it became primary (§6.6).
    #[error("this replica of shard {0} does not lead it, or does not serve yet")]
    NotLeading(ShardRef),
    /// The index could not be read.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// The blocking pool is shut down.
    #[error("the index's blocking pool is shut down")]
    PoolClosed,
}

/// A bug to seed in a judge, for tests that show the cluster simulation
/// catches it.
#[cfg(feature = "test-util")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JudgeBug {
    /// Judges every attempt as not in progress, without the tracker.
    IgnoreAttempts,
    /// Answers as soon as the replica is primary, before it serves.
    BeforeServing,
    /// Judges attempts of later epochs like those of earlier ones.
    IgnoreLaterEpochs,
}

/// The orphan verdicts of one shard's replica on this node, given only
/// while it leads the shard (see [the module](crate::orphans) for the
/// fence).
pub struct OrphanJudge<D: Disk> {
    shard: Shard<D>,
    attempts: Attempts,
    pool: BlockingPool,
    #[cfg(feature = "test-util")]
    bug: Option<JudgeBug>,
}

impl<D: Disk> Clone for OrphanJudge<D> {
    fn clone(&self) -> Self {
        Self {
            shard: self.shard.clone(),
            attempts: self.attempts.clone(),
            pool: self.pool.clone(),
            #[cfg(feature = "test-util")]
            bug: self.bug,
        }
    }
}

impl<D: Disk> std::fmt::Debug for OrphanJudge<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrphanJudge")
            .field("shard", self.shard.shard())
            .finish_non_exhaustive()
    }
}

impl<D: Disk> OrphanJudge<D> {
    /// The judge of `shard`'s replica, fenced by `attempts`, the tracker
    /// every fragment-writing attempt of the shard on this node shares,
    /// and reading the index on `pool`.
    #[must_use]
    pub fn new(shard: Shard<D>, attempts: Attempts, pool: BlockingPool) -> Self {
        Self {
            shard,
            attempts,
            pool,
            #[cfg(feature = "test-util")]
            bug: None,
        }
    }

    /// The same judge with `bug` seeded.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_bug(mut self, bug: Option<JudgeBug>) -> Self {
        self.bug = bug;
        self
    }

    /// The shard.
    #[must_use]
    pub fn shard(&self) -> &ShardRef {
        self.shard.shard()
    }

    /// The verdicts on `suspects`, fragments that `node` holds, one per
    /// suspect and in their order. An orphan's attempt is abandoned in the
    /// tracker before the verdict is returned, so it never publishes.
    ///
    /// # Errors
    ///
    /// [`JudgeError::NotLeading`] unless the replica leads the shard and
    /// serves; [`JudgeError::Index`] if the index cannot be read.
    pub async fn judge(
        &self,
        node: &NodeId,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, JudgeError> {
        if !self.leads() {
            return Err(JudgeError::NotLeading(self.shard().clone()));
        }
        let epoch = self.shard.sequencing();
        // Publishing records at or before the applied position committed.
        self.attempts.settle(self.shard.applied());
        let mut verdicts = vec![Verdict::InProgress; suspects.len()];
        let mut decide = Vec::new();
        for (at, suspect) in suspects.iter().enumerate() {
            let orphaned = if self.has_bug_ignore_attempts() {
                true
            } else if self.has_bug_ignore_later_epochs() {
                self.attempts
                    .orphaned(suspect.attempt, epoch.max(suspect.attempt.epoch))
            } else {
                self.attempts.orphaned(suspect.attempt, epoch)
            };
            if orphaned {
                decide.push(at);
            }
        }
        if decide.is_empty() {
            return Ok(verdicts);
        }
        let keys: Vec<String> = decide.iter().map(|&at| suspects[at].key.clone()).collect();
        let entries = self.entries(keys).await?;
        for &at in &decide {
            let suspect = &suspects[at];
            let referenced = entries
                .get(&suspect.key)
                .is_some_and(|entry| references(entry.as_ref(), node, suspect));
            verdicts[at] = if referenced {
                Verdict::Referenced
            } else {
                Verdict::Orphan
            };
        }
        Ok(verdicts)
    }

    /// Whether the replica leads the shard and serves.
    fn leads(&self) -> bool {
        let primary = matches!(self.shard.role(), Role::Primary | Role::Alone);
        primary && (self.shard.is_serving() || self.has_bug_before_serving())
    }

    /// The applied entries of `keys`, read in one index transaction.
    async fn entries(
        &self,
        keys: Vec<String>,
    ) -> Result<BTreeMap<String, Option<Entry>>, JudgeError> {
        let index: Arc<Index> = Arc::clone(self.shard.index());
        let shard = self.shard().clone();
        self.pool
            .run(move || {
                let reader = index.read()?;
                keys.into_iter()
                    .map(|key| {
                        let entry = reader.entry(&shard, &key)?;
                        Ok((key, entry))
                    })
                    .collect::<Result<BTreeMap<_, _>, IndexError>>()
            })
            .await
            .map_err(|_| JudgeError::PoolClosed)?
            .map_err(JudgeError::from)
    }

    #[cfg(feature = "test-util")]
    fn has_bug_ignore_attempts(&self) -> bool {
        self.bug == Some(JudgeBug::IgnoreAttempts)
    }

    #[cfg(not(feature = "test-util"))]
    fn has_bug_ignore_attempts(&self) -> bool {
        false
    }

    #[cfg(feature = "test-util")]
    fn has_bug_before_serving(&self) -> bool {
        self.bug == Some(JudgeBug::BeforeServing)
    }

    #[cfg(not(feature = "test-util"))]
    fn has_bug_before_serving(&self) -> bool {
        false
    }

    #[cfg(feature = "test-util")]
    fn has_bug_ignore_later_epochs(&self) -> bool {
        self.bug == Some(JudgeBug::IgnoreLaterEpochs)
    }

    #[cfg(not(feature = "test-util"))]
    fn has_bug_ignore_later_epochs(&self) -> bool {
        false
    }
}

/// Whether `entry`'s coded layout names `suspect` on `node`.
fn references(entry: Option<&Entry>, node: &NodeId, suspect: &Suspect) -> bool {
    let Some(coded) = entry
        .and_then(|entry| entry.object.as_ref())
        .and_then(|object| object.coded.as_ref())
    else {
        return false;
    };
    coded.stripes.iter().any(|stripe| {
        stripe
            .fragments()
            .iter()
            .any(|location| location.node == *node && location.fragment == suspect.id)
    })
}
