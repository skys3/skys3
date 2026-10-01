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
    RegisterKey, RegisterKind, RetryPolicy, TypedKey, Version, bump_generation, propose,
    propose_delete,
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
    /// The generation that announces the writes, or `None` if nothing was
    /// written.
    pub generation: Option<Generation>,
}

impl Applied {
    /// Whether every write of the change took effect.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.rejected.is_none()
    }
}

/// Applies `change`: sends its writes in order, each with the retries and
/// the lost-response rule of [`propose`], stops at the first one whose
/// precondition fails, and then increments the generation in
/// `cluster.json` to announce what was written (design §6.2). The
/// increment comes after the writes, so a node that sees the new
/// generation and then lists the registers finds them.
///
/// A change whose first write is rejected writes nothing and is not
/// announced. The planner re-reads the registers and plans again.
///
/// # Errors
///
/// The first write that failed without an answer that settles it, after
/// `policy`'s retries. If an earlier write took effect, or the failed one
/// may have, the generation is still incremented first, so the next
/// observer of `cluster.json` finds the write; if that increment fails too,
/// the next change's increment announces it.
pub async fn apply<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    change: &ChangeSet,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Applied, ControlError> {
    let mut written = Vec::new();
    let mut rejected = None;
    let mut failure = None;
    for write in &change.writes {
        let result = match &write.operation {
            Operation::Put {
                expected,
                value,
                proposal,
            } => propose(
                store,
                &write.key,
                expected.clone(),
                value.clone(),
                proposal,
                policy,
            )
            .await
            .map(|outcome| match outcome {
                ProposalOutcome::Accepted(version) => Some(Some(version)),
                ProposalOutcome::Rejected => None,
            }),
            Operation::Delete { expected } => propose_delete(store, &write.key, expected, policy)
                .await
                .map(|outcome| match outcome {
                    DeletionOutcome::Deleted => Some(None),
                    DeletionOutcome::Rejected => None,
                }),
        };
        match result {
            Ok(Some(version)) => written.push((write.key.clone(), version)),
            Ok(None) => {
                rejected = Some(write.key.clone());
                break;
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    let uncertain = failure.as_ref().is_some_and(ControlError::may_have_applied);
    let generation = if written.is_empty() && !uncertain {
        None
    } else {
        match bump_generation(store, cluster, proposals, policy).await {
            Ok(generation) => Some(generation),
            Err(error) => return Err(failure.unwrap_or(error)),
        }
    };
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(Applied {
        written,
        rejected,
        generation,
    })
}

#[cfg(test)]
mod tests {
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
        assert!(change.writes().iter().all(|w| w.expected() == Expected::Absent));
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

    #[tokio::test(start_paused = true)]
    async fn a_write_that_may_have_landed_is_announced_and_reported() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let mut ids = ProposalIds::seeded(3);
        bootstrapped(&store, &mut ids).await;
        // Every attempt is applied and its answer lost: the lost-response
        // rule's re-read is lost too, so the outcome stays unknown.
        store.script([Fault::LoseResponse, Fault::LoseRequest, Fault::LoseRequest]);
        let change = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap();
        let error = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap_err();
        assert!(error.may_have_applied(), "{error}");
        assert_eq!(generation(&store).await, Generation::new(2));
        assert!(read(&store, &shard_key(0)).await.unwrap().is_some());

        // A failure that applied nothing is not announced.
        store.script([Fault::Unavailable, Fault::Unavailable, Fault::Unavailable]);
        let change = ChangeSet::new()
            .create(&shard_key(1), &shard(1, &mut ids))
            .unwrap();
        let error = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap_err();
        assert!(!error.may_have_applied(), "{error}");
        assert_eq!(generation(&store).await, Generation::new(2));

        // An empty change does nothing.
        let applied = apply(&store, &cluster(), &ChangeSet::new(), &mut ids, &quick())
            .await
            .unwrap();
        assert_eq!(applied.generation, None);
        assert!(ChangeSet::new().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_announcement_reports_the_write_failure_first() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let mut ids = ProposalIds::seeded(4);
        bootstrapped(&store, &mut ids).await;
        // The write lands unseen, and then the store stops answering.
        store.script(
            [Fault::LoseResponse]
                .into_iter()
                .chain(std::iter::repeat_n(Fault::Unavailable, 6)),
        );
        let change = ChangeSet::new()
            .create(&shard_key(0), &shard(1, &mut ids))
            .unwrap();
        let error = apply(&store, &cluster(), &change, &mut ids, &quick())
            .await
            .unwrap_err();
        assert!(error.may_have_applied(), "{error}");
        assert_eq!(generation(&store).await, FIRST);
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
