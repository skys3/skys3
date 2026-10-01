//! The coordinator's change path (design §6.2, §6.7): a change is a
//! sequence of compare-and-swaps on registers, followed by an increment of
//! the generation in `cluster.json` that announces it.
//!
//! A [`ChangeSet`] can only hold conditional writes: a create under
//! `If-None-Match: *`, or an update or delete under `If-Match` on a version
//! the planner read. So a coordinator that acts on a stale view, or a
//! second node that wrongly believes it is coordinator, can only lose a
//! race; it can never overwrite a value it has not seen.

use bytes::Bytes;
use skys3_control::{
    ControlError, ControlStore, DeletionOutcome, Expected, ProposalIds, ProposalOutcome,
    RegisterKey, RegisterKind, RetryPolicy, TypedKey, Version, bump_generation, get_with_retries,
    proposal_id_of, propose, propose_delete,
};
use skys3_types::{ClusterId, Generation, ProposalId, RegisterDocument, RegisterError};

/// One conditional write of a [`ChangeSet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    key: RegisterKey,
    operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Operation {
    Put {
        expected: Expected,
        value: Bytes,
        proposal: ProposalId,
    },
    Delete {
        expected: Version,
    },
}

impl Write {
    /// The register written.
    #[must_use]
    pub fn key(&self) -> &RegisterKey {
        &self.key
    }

    /// The write's precondition: absent for a create, the version the
    /// planner read otherwise.
    #[must_use]
    pub fn expected(&self) -> Expected {
        match &self.operation {
            Operation::Put { expected, .. } => expected.clone(),
            Operation::Delete { expected } => Expected::Version(expected.clone()),
        }
    }

    /// Whether the write deletes the register.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self.operation, Operation::Delete { .. })
    }
}

/// Why a write cannot be part of a change.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChangeError {
    /// `cluster.json` and `coordinator.lease` are written only by the
    /// change path itself and by the lease.
    #[error("{0} is not written by coordinator changes")]
    Reserved(RegisterKey),
    /// The document breaks an invariant of its register.
    #[error("register {key} would be invalid: {source}")]
    Invalid {
        /// The register.
        key: RegisterKey,
        /// What is wrong with the document.
        #[source]
        source: RegisterError,
    },
}

/// A change the coordinator makes: conditional register writes, applied in
/// order by [`apply`].
///
/// Each document carries a fresh `proposal_id`, which the planner draws
/// from the [`ProposalIds`] it is given, so the lost-response rule can
/// tell whether an unanswered write landed (design §6.1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ChangeSet {
    writes: Vec<Write>,
}

impl ChangeSet {
    /// An empty change.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a write that creates the register, which must not exist.
    ///
    /// # Errors
    ///
    /// [`ChangeError`] for a reserved register or an invalid document.
    pub fn create<D: RegisterDocument>(
        self,
        key: &TypedKey<D>,
        document: &D,
    ) -> Result<Self, ChangeError> {
        self.put(key, Expected::Absent, document)
    }

    /// Adds a write that replaces the register's value, which must still
    /// be at `read`, the version the planner read it at.
    ///
    /// # Errors
    ///
    /// [`ChangeError`] for a reserved register or an invalid document.
    pub fn update<D: RegisterDocument>(
        self,
        key: &TypedKey<D>,
        read: &Version,
        document: &D,
    ) -> Result<Self, ChangeError> {
        self.put(key, Expected::Version(read.clone()), document)
    }

    /// Adds a write that deletes the register, which must still be at
    /// `read`.
    ///
    /// # Errors
    ///
    /// [`ChangeError::Reserved`] for a reserved register.
    pub fn delete<D: RegisterDocument>(
        mut self,
        key: &TypedKey<D>,
        read: &Version,
    ) -> Result<Self, ChangeError> {
        let key = writable(key.key())?;
        self.writes.push(Write {
            key,
            operation: Operation::Delete {
                expected: read.clone(),
            },
        });
        Ok(self)
    }

    fn put<D: RegisterDocument>(
        mut self,
        key: &TypedKey<D>,
        expected: Expected,
        document: &D,
    ) -> Result<Self, ChangeError> {
        let key = writable(key.key())?;
        let value = document.to_json().map_err(|source| ChangeError::Invalid {
            key: key.clone(),
            source,
        })?;
        self.writes.push(Write {
            key,
            operation: Operation::Put {
                expected,
                value: Bytes::from(value),
                proposal: document.proposal_id().clone(),
            },
        });
        Ok(self)
    }

    /// The writes, in the order they are applied.
    #[must_use]
    pub fn writes(&self) -> &[Write] {
        &self.writes
    }

    /// Whether the change writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

fn writable(key: &RegisterKey) -> Result<RegisterKey, ChangeError> {
    match key.kind() {
        RegisterKind::Cluster | RegisterKind::CoordinatorLease => {
            Err(ChangeError::Reserved(key.clone()))
        }
        _ => Ok(key.clone()),
    }
}

/// What [`apply`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Applied {
    /// The writes that took effect, in order, with the version each
    /// register now holds, or `None` for a deleted one.
    pub written: Vec<(RegisterKey, Option<Version>)>,
    /// The register whose precondition failed: another writer changed it
    /// since it was read. The writes after it were not sent.
    pub rejected: Option<RegisterKey>,
    /// The register whose write failed: without an answer, or with an
    /// error a retry does not fix. The writes after it were not sent.
    pub failed: Option<RegisterKey>,
    /// The generation that announces `written`, or `None` if nothing was
    /// written or the increment failed.
    pub generation: Option<Generation>,
    /// What is still to be settled and announced, if anything: see
    /// [`Pending`].
    pub pending: Option<Pending>,
}

impl Applied {
    /// Whether every write of the change took effect.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.rejected.is_none() && self.failed.is_none()
    }
}

/// A change that failed part of the way, with what it did before it
/// failed: [`ChangeFailed::applied`] may hold writes that took effect and
/// the generation that announces them, which the caller still pushes.
#[derive(Debug, thiserror::Error)]
#[error("a coordinator change failed: {source}")]
pub struct ChangeFailed {
    /// Why the change failed: a write's error, or the generation
    /// increment's.
    #[source]
    pub source: ControlError,
    /// What the change did before it failed.
    pub applied: Applied,
}

/// The part of a change whose announcement is still owed (design §6.7):
/// a write that got no answer that settles it, so that it may still land
/// later, or writes that landed while the generation increment failed.
///
/// Announcing a write before its outcome is known would let a node read
/// the new generation and list the registers before the write lands, and
/// miss it until some later change. So the write is settled first:
/// [`settle`] sends it again under the same precondition until an answer
/// comes, after which no attempt of it can land any more, and then
/// increments the generation if it took effect.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Pending {
    /// The write whose outcome is unknown.
    write: Option<Write>,
    /// Whether writes that landed still wait for their generation
    /// increment.
    unannounced: bool,
}

impl Pending {
    /// The register of the write whose outcome is unknown, if any.
    #[must_use]
    pub fn unsettled(&self) -> Option<&RegisterKey> {
        self.write.as_ref().map(Write::key)
    }
}

/// What [`settle`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Settled {
    /// The unsettled write, if it took effect, with the version its
    /// register now holds, or `None` for a deleted one.
    pub written: Option<(RegisterKey, Option<Version>)>,
    /// The generation that announces what landed, or `None` if nothing
    /// was owed.
    pub generation: Option<Generation>,
}

/// Applies `change`: sends its writes in order, each with the retries and
/// the lost-response rule of [`propose`], stops at the first one that does
/// not take effect, and then increments the generation in `cluster.json`
/// to announce what was written (design §6.2). The increment comes after
/// the writes, so a node that sees the new generation and then lists the
/// registers finds them.
///
/// A change whose first write is rejected writes nothing and is not
/// announced. The planner re-reads the registers and plans again.
///
/// # Errors
///
/// A boxed [`ChangeFailed`] if a write failed after `policy`'s retries, or the
/// increment did. Its [`Applied`] holds the writes that took effect before
/// and the generation that announces them, and a [`Pending`] if a write
/// may still land or an announcement is owed: the caller [`settle`]s it.
pub async fn apply<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    change: &ChangeSet,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Applied, Box<ChangeFailed>> {
    let mut written = Vec::new();
    let (mut rejected, mut failed, mut failure, mut unsettled) = (None, None, None, None);
    for write in &change.writes {
        match send(store, write, policy).await {
            Ok(Some(version)) => written.push((write.key.clone(), version)),
            Ok(None) => {
                rejected = Some(write.key.clone());
                break;
            }
            Err(error) => {
                failed = Some(write.key.clone());
                if error.may_have_applied() {
                    unsettled = Some(write.clone());
                }
                failure = Some(error);
                break;
            }
        }
    }
    let mut generation = None;
    if !written.is_empty() {
        match bump_generation(store, cluster, proposals, policy).await {
            Ok(bumped) => generation = Some(bumped),
            Err(error) => failure = failure.or(Some(error)),
        }
    }
    let unannounced = !written.is_empty() && generation.is_none();
    let pending = (unsettled.is_some() || unannounced).then_some(Pending {
        write: unsettled,
        unannounced,
    });
    let applied = Applied {
        written,
        rejected,
        failed,
        generation,
        pending,
    };
    match failure {
        Some(source) => Err(Box::new(ChangeFailed { source, applied })),
        None => Ok(applied),
    }
}

/// Settles `pending`: learns whether its write took effect, sending it
/// again under the same precondition so that no earlier attempt can land
/// after the answer, and increments the generation if the write took
/// effect or an announcement was owed.
///
/// # Errors
///
/// The store's error, after `policy`'s retries. The caller keeps
/// `pending` and settles it again later; settling twice is harmless.
pub async fn settle<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    pending: &Pending,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Settled, ControlError> {
    let written = match &pending.write {
        Some(write) => outcome(store, write, policy)
            .await?
            .map(|version| (write.key.clone(), version)),
        None => None,
    };
    let generation = if written.is_some() || pending.unannounced {
        Some(bump_generation(store, cluster, proposals, policy).await?)
    } else {
        None
    };
    Ok(Settled {
        written,
        generation,
    })
}

/// Sends one write: `Some` with the register's new version (`None` once
/// deleted) if it took effect, `None` if its precondition failed.
async fn send<S: ControlStore>(
    store: &S,
    write: &Write,
    policy: &RetryPolicy,
) -> Result<Option<Option<Version>>, ControlError> {
    match &write.operation {
        Operation::Put {
            expected,
            value,
            proposal,
        } => {
            let outcome = propose(
                store,
                &write.key,
                expected.clone(),
                value.clone(),
                proposal,
                policy,
            )
            .await?;
            Ok(match outcome {
                ProposalOutcome::Accepted(version) => Some(Some(version)),
                ProposalOutcome::Rejected => None,
            })
        }
        Operation::Delete { expected } => {
            let outcome = propose_delete(store, &write.key, expected, policy).await?;
            Ok(match outcome {
                DeletionOutcome::Deleted => Some(None),
                DeletionOutcome::Rejected => None,
            })
        }
    }
}

/// The final outcome of a write an earlier attempt of which got no answer:
/// it is sent again, and a failed precondition is resolved by reading the
/// register. Either answer means no attempt can land any more: the
/// register has moved past the version the write expected, and versions
/// are never reused (design §6.1).
async fn outcome<S: ControlStore>(
    store: &S,
    write: &Write,
    policy: &RetryPolicy,
) -> Result<Option<Option<Version>>, ControlError> {
    if let Some(version) = send(store, write, policy).await? {
        return Ok(Some(version));
    }
    let current = get_with_retries(store, &write.key, policy).await?;
    Ok(match (&write.operation, current) {
        (Operation::Put { proposal, .. }, Some(current))
            if proposal_id_of(&current.value).as_ref() == Some(proposal) =>
        {
            Some(Some(current.version))
        }
        // Gone: deleted by this write or another one, and either way the
        // version it expected no longer exists.
        (Operation::Delete { .. }, None) => Some(None),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use skys3_control::faults::{Fault, FaultyStore};
    use skys3_control::{MemoryControlStore, bootstrap, read, read_cluster};
    use skys3_types::{BucketId, Epoch, NodeId, ShardConfig, ShardId};

    use super::*;

    fn cluster() -> ClusterId {
        ClusterId::new("prod").unwrap()
    }

    fn quick() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        }
    }

    fn shard(epoch: u64, ids: &mut ProposalIds) -> ShardConfig {
        let node = NodeId::new("node-1").unwrap();
        ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
            epoch: Epoch::new(epoch),
            primary: node.clone(),
            members: vec![node],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: ids.next_id(),
        }
    }

    fn shard_key(n: u8) -> TypedKey<ShardConfig> {
        TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(n))
    }

    async fn bootstrapped<S: ControlStore>(store: &S, ids: &mut ProposalIds) {
        bootstrap(store, &cluster(), ids.next_id(), &quick())
            .await
            .unwrap();
    }

    async fn generation<S: ControlStore>(store: &S) -> Generation {
        read_cluster(store, &cluster(), &quick())
            .await
            .unwrap()
            .value
            .generation
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_writes_in_order_and_increments_the_generation_once() {
        let store = MemoryControlStore::new();
        let mut ids = ProposalIds::seeded(1);
        bootstrapped(&store, &mut ids).await;
        let change = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap()
            .create(&shard_key(1), &shard(1, &mut ids))
            .unwrap();
        assert_eq!(change.writes().len(), 2);
        assert!(
            change
                .writes()
                .iter()
                .all(|w| w.expected() == Expected::Absent)
        );
        let applied = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap();
        assert!(applied.is_complete());
        assert_eq!(applied.generation, Some(Generation::new(2)));
        assert_eq!(
            applied.written.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            [shard_key(0).key(), shard_key(1).key()]
        );
        assert_eq!(generation(&store).await, Generation::new(2));

        // An update and a delete, at the versions just read.
        let first = read(&store, &shard_key(0)).await.unwrap().unwrap();
        let second = read(&store, &shard_key(1)).await.unwrap().unwrap();
        let change = ChangeSet::new()
            .update(&shard_key(0), &first.version, &shard(2, &mut ids))
            .unwrap()
            .delete(&shard_key(1), &second.version)
            .unwrap();
        assert!(change.writes()[1].is_delete());
        assert_eq!(
            change.writes()[1].expected(),
            Expected::Version(second.version)
        );
        let applied = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(applied.generation, Some(Generation::new(3)));
        assert_eq!(applied.written[1], (shard_key(1).key().clone(), None));
        let updated = read(&store, &shard_key(0)).await.unwrap().unwrap();
        assert_eq!(updated.value.epoch, Epoch::new(2));
        assert_eq!(applied.written[0].1, Some(updated.version));
        assert!(read(&store, &shard_key(1)).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_change_stops_at_the_register_another_writer_changed() {
        let store = MemoryControlStore::new();
        let mut ids = ProposalIds::seeded(2);
        bootstrapped(&store, &mut ids).await;
        let setup = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap();
        let _ = apply(&store, &cluster(), &setup, &mut ids, &quick())
            .await
            .unwrap();
        let stale = read(&store, &shard_key(0)).await.unwrap().unwrap();
        // Another coordinator moves the register on.
        let other = ChangeSet::new()
            .update(&shard_key(0), &stale.version, &shard(2, &mut ids))
            .unwrap();
        let _ = apply(&store, &cluster(), &other, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(generation(&store).await, Generation::new(3));

        // A change planned from the stale read loses, and writes nothing
        // after it.
        let change = ChangeSet::new()
            .update(&shard_key(0), &stale.version, &shard(2, &mut ids))
            .unwrap()
            .create(&shard_key(1), &shard(1, &mut ids))
            .unwrap();
        let applied = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(applied.rejected.as_ref(), Some(shard_key(0).key()));
        assert!(!applied.is_complete());
        assert!(applied.written.is_empty() && applied.generation.is_none());
        assert!(read(&store, &shard_key(1)).await.unwrap().is_none());
        assert_eq!(generation(&store).await, Generation::new(3));

        // A partly applied change is still announced.
        let current = read(&store, &shard_key(0)).await.unwrap().unwrap();
        let change = ChangeSet::new()
            .create(&shard_key(2), &shard(1, &mut ids))
            .unwrap()
            .update(&shard_key(0), &stale.version, &shard(3, &mut ids))
            .unwrap();
        let applied = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(applied.written.len(), 1);
        assert_eq!(applied.rejected.as_ref(), Some(shard_key(0).key()));
        assert_eq!(applied.generation, Some(Generation::new(4)));
        let unchanged = read(&store, &shard_key(0)).await.unwrap().unwrap();
        assert_eq!(unchanged.version, current.version);
    }

    /// Applies `change`, which must fail, and returns what it did.
    async fn failing<S: ControlStore>(
        store: &S,
        change: &ChangeSet,
        ids: &mut ProposalIds,
    ) -> Box<ChangeFailed> {
        apply(store, &cluster(), change, ids, &quick())
            .await
            .unwrap_err()
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_may_still_land_is_announced_only_once_settled() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let mut ids = ProposalIds::seeded(3);
        bootstrapped(&store, &mut ids).await;
        // The first attempt is delayed in the network and lands a second
        // later; the retries are lost.
        let late = Fault::LateRequest(Duration::from_secs(1));
        store.script([late.clone(), Fault::LoseRequest, Fault::LoseRequest]);
        let change = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap();
        let failed = failing(&store, &change, &mut ids).await;
        assert!(failed.source.may_have_applied(), "{failed}");
        assert!(
            failed
                .to_string()
                .starts_with("a coordinator change failed")
        );
        assert_eq!(failed.applied.failed.as_ref(), Some(shard_key(0).key()));
        assert!(!failed.applied.is_complete());
        let pending = failed.applied.pending.unwrap();
        assert_eq!(pending.unsettled(), Some(shard_key(0).key()));
        // Nothing is announced while the write may still land: a node that
        // saw a new generation now would list the registers without it.
        assert_eq!(generation(&store).await, FIRST);
        assert!(read(&store, &shard_key(0)).await.unwrap().is_none());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(read(&store, &shard_key(0)).await.unwrap().is_some());
        assert_eq!(generation(&store).await, FIRST);

        // Settling finds the write and announces it.
        let settled = settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap();
        let version = read(&store, &shard_key(0)).await.unwrap().unwrap().version;
        assert_eq!(
            settled.written,
            Some((shard_key(0).key().clone(), Some(version)))
        );
        assert_eq!(settled.generation, Some(Generation::new(2)));
        assert_eq!(generation(&store).await, Generation::new(2));

        // A write none of whose attempts landed lands when it is settled.
        store.script([Fault::LoseRequest, Fault::LoseRequest, Fault::LoseRequest]);
        let change = ChangeSet::new()
            .create(&shard_key(1), &shard(1, &mut ids))
            .unwrap();
        let pending = failing(&store, &change, &mut ids)
            .await
            .applied
            .pending
            .unwrap();
        let settled = settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(settled.generation, Some(Generation::new(3)));
        assert!(read(&store, &shard_key(1)).await.unwrap().is_some());

        // A store that does not answer leaves it pending.
        store.script([Fault::LoseRequest, Fault::LoseRequest, Fault::LoseRequest]);
        let change = ChangeSet::new()
            .create(&shard_key(2), &shard(1, &mut ids))
            .unwrap();
        let pending = failing(&store, &change, &mut ids)
            .await
            .applied
            .pending
            .unwrap();
        store.script(std::iter::repeat_n(Fault::Unavailable, 3));
        settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap_err();

        // Another writer got there first: nothing landed, nothing is owed.
        let theirs = ChangeSet::new()
            .create(&shard_key(2), &shard(7, &mut ids))
            .unwrap();
        let _ = apply(&store, &cluster(), &theirs, &mut ids, &quick())
            .await
            .unwrap();
        let settled = settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(settled.written, None);
        assert_eq!(settled.generation, None);
        let stored = read(&store, &shard_key(2)).await.unwrap().unwrap();
        assert_eq!(stored.value.epoch, Epoch::new(7));
        assert_eq!(generation(&store).await, Generation::new(4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_delete_that_may_still_land_is_settled_by_its_absence() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let mut ids = ProposalIds::seeded(6);
        bootstrapped(&store, &mut ids).await;
        let setup = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap();
        let _ = apply(&store, &cluster(), &setup, &mut ids, &quick())
            .await
            .unwrap();
        let current = read(&store, &shard_key(0)).await.unwrap().unwrap();
        let late = Fault::LateRequest(Duration::from_secs(1));
        store.script([late, Fault::LoseRequest, Fault::LoseRequest]);
        let change = ChangeSet::new()
            .delete(&shard_key(0), &current.version)
            .unwrap();
        let pending = failing(&store, &change, &mut ids)
            .await
            .applied
            .pending
            .unwrap();
        assert_eq!(generation(&store).await, Generation::new(2));
        tokio::time::sleep(Duration::from_secs(2)).await;
        let settled = settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(settled.written, Some((shard_key(0).key().clone(), None)));
        assert_eq!(settled.generation, Some(Generation::new(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_partly_applied_change_keeps_the_generation_that_announces_it() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let mut ids = ProposalIds::seeded(4);
        bootstrapped(&store, &mut ids).await;
        // The first write lands; the second is refused every time.
        store.script(
            [Fault::Pass]
                .into_iter()
                .chain(std::iter::repeat_n(Fault::Unavailable, 3)),
        );
        let change = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap()
            .create(&shard_key(1), &shard(1, &mut ids))
            .unwrap();
        let failed = failing(&store, &change, &mut ids).await;
        assert!(!failed.source.may_have_applied(), "{failed}");
        let applied = failed.applied;
        assert_eq!(applied.written.len(), 1);
        assert_eq!(applied.failed.as_ref(), Some(shard_key(1).key()));
        assert_eq!(applied.generation, Some(Generation::new(2)));
        assert_eq!(applied.pending, None);
        assert_eq!(generation(&store).await, Generation::new(2));

        // Writes that land while the increment fails are owed one.
        store.script(
            [Fault::Pass]
                .into_iter()
                .chain(std::iter::repeat_n(Fault::Unavailable, 3)),
        );
        let change = ChangeSet::new()
            .create(&shard_key(2), &shard(1, &mut ids))
            .unwrap();
        let failed = failing(&store, &change, &mut ids).await;
        assert!(failed.applied.is_complete());
        assert_eq!(failed.applied.generation, None);
        let pending = failed.applied.pending.unwrap();
        assert_eq!(pending.unsettled(), None);
        assert_eq!(generation(&store).await, Generation::new(2));
        let settled = settle(&store, &cluster(), &pending, &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(settled.written, None);
        assert_eq!(settled.generation, Some(Generation::new(3)));

        // A failure that applied nothing is neither announced nor owed.
        store.script(std::iter::repeat_n(Fault::Unavailable, 3));
        let change = ChangeSet::new()
            .create(&shard_key(3), &shard(1, &mut ids))
            .unwrap();
        let failed = failing(&store, &change, &mut ids).await;
        assert_eq!(failed.applied.generation, None);
        assert_eq!(failed.applied.pending, None);
        assert_eq!(generation(&store).await, Generation::new(3));

        // An empty change does nothing.
        let applied = apply(&store, &cluster(), &ChangeSet::new(), &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(applied.generation, None);
        assert!(ChangeSet::new().is_empty());
    }

    const FIRST: Generation = Generation::new(1);

    #[test]
    fn the_lease_and_the_generation_are_not_change_writes() {
        let mut ids = ProposalIds::seeded(5);
        let cluster_doc = skys3_types::ClusterDocument {
            cluster_id: cluster(),
            format_version: skys3_types::ClusterDocument::FORMAT_VERSION,
            generation: FIRST,
            proposal_id: ids.next_id(),
        };
        let error = ChangeSet::new()
            .create(&TypedKey::cluster(), &cluster_doc)
            .unwrap_err();
        assert!(matches!(error, ChangeError::Reserved(_)), "{error}");
        let error = ChangeSet::new()
            .delete(&TypedKey::coordinator_lease(), &Version::new("1"))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "coordinator.lease is not written by coordinator changes"
        );
        let mut invalid = shard(1, &mut ids);
        invalid.members.clear();
        let error = ChangeSet::new()
            .create(&shard_key(0), &invalid)
            .unwrap_err();
        assert!(matches!(error, ChangeError::Invalid { .. }), "{error}");
    }
}
