//! Draining a node (design §6.7): an operator marks the node's
//! registration `draining`, and placement then treats the node as it
//! treats a departing one. It assigns the node nothing, replacement adds
//! learners elsewhere for the shards the node is a member of and removes
//! it once they are promoted, and rebalancing hands off the shards it
//! leads. Unlike a coordinator's `departing` mark, which a node clears by
//! registering again, the node keeps a drain when it registers
//! ([`register`](crate::register)), restarts included: only an operator
//! clears it, with [`set_draining`] again.
//!
//! The mark is a compare-and-swap on `nodes/<node-id>.json` through the
//! coordinator's change path ([`apply`]), announced by a generation
//! increment once it has landed, as every registration write is. Whoever
//! can write the control store can make it, which is the store's own
//! trust (design §12): no admin message carries it.

use skys3_control::{ControlError, ControlStore, ProposalIds, RetryPolicy, TypedKey};
use skys3_types::{ClusterId, Generation, NodeId, NodeRegistration};

use crate::change::{ChangeError, ChangeSet, apply, settle};

/// How many times [`set_draining`] re-reads a registration another writer
/// changed under it before it gives up.
const MAX_ROUNDS: usize = 8;

/// The outcome of a successful [`set_draining`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    /// The node's registration as it now stands.
    pub registration: NodeRegistration,
    /// Whether the mark changed: `false` if the registration already said
    /// what was asked.
    pub changed: bool,
    /// The generation that announces the change, if there was one and it
    /// was announced.
    pub generation: Option<Generation>,
}

/// Why [`set_draining`] did not mark the node.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DrainError {
    /// No registration names the node.
    #[error("node {0} is not registered")]
    Unregistered(NodeId),
    /// The control store failed, or holds a registration this build
    /// cannot read, which is never overwritten unseen.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// The marked registration is not a valid change.
    #[error(transparent)]
    Invalid(#[from] ChangeError),
    /// Other writers kept changing the registration.
    #[error("the registration of {0} kept changing")]
    Contended(NodeId),
    /// The write got no answer that settles it, and settling it failed:
    /// the mark may or may not have landed. Asking again finds out.
    #[error("the outcome of the write is unknown: {0}")]
    Unsettled(ControlError),
}

/// Marks node `node`'s registration `draining` (or clears the mark, with
/// `draining` false) by a compare-and-swap, announced in `cluster`'s
/// generation. A registration that already says so is left alone.
///
/// # Errors
///
/// [`DrainError`]: the node is not registered, or the store failed.
pub async fn set_draining<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    node: &NodeId,
    draining: bool,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Drained, DrainError> {
    let key = TypedKey::node(node);
    for _ in 0..MAX_ROUNDS {
        let Some(current) = skys3_control::read_with_retries(store, &key, policy).await? else {
            return Err(DrainError::Unregistered(node.clone()));
        };
        if current.value.draining == draining {
            return Ok(Drained {
                registration: current.value,
                changed: false,
                generation: None,
            });
        }
        let mut marked = current.value.clone();
        marked.draining = draining;
        marked.proposal_id = proposals.next_id();
        let change = ChangeSet::new().update(&key, &current.version, &marked)?;
        let (landed, generation) = match apply(store, cluster, &change, proposals, policy).await {
            Ok(applied) => (!applied.written.is_empty(), applied.generation),
            Err(failed) => {
                let Some(pending) = failed.applied.pending else {
                    return Err(failed.source.into());
                };
                let settled = settle(store, cluster, &pending, proposals, policy)
                    .await
                    .map_err(DrainError::Unsettled)?;
                (
                    !failed.applied.written.is_empty() || settled.written.is_some(),
                    settled.generation.or(failed.applied.generation),
                )
            }
        };
        if landed {
            tracing::info!(%node, draining, "changed the node's drain mark");
            return Ok(Drained {
                registration: marked,
                changed: true,
                generation,
            });
        }
        // Another writer changed the registration, such as the node
        // registering again: decide again from what it holds now.
    }
    Err(DrainError::Contended(node.clone()))
}

#[cfg(test)]
mod tests {
    use skys3_control::faults::{Fault, FaultyStore};
    use skys3_control::{MemoryControlStore, bootstrap, read, read_cluster};
    use skys3_types::{DiskInfo, Label};

    use super::*;
    use crate::join::{NodeProfile, Registration, register};

    fn cluster() -> ClusterId {
        ClusterId::new("prod").unwrap()
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            initial_backoff: std::time::Duration::from_millis(1),
            max_backoff: std::time::Duration::from_millis(10),
        }
    }

    fn profile() -> NodeProfile {
        NodeProfile {
            node: NodeId::new("node-1").unwrap(),
            address: "10.0.0.1:7400".parse().unwrap(),
            zone: None,
            rack: Some(Label::new("rack-1").unwrap()),
            disks: vec![DiskInfo {
                disk_id: Label::new("nvme0").unwrap(),
                capacity_bytes: 1 << 40,
            }],
            instance: None,
        }
    }

    async fn registered() -> MemoryControlStore {
        let store = MemoryControlStore::new();
        bootstrap(
            &store,
            &cluster(),
            ProposalIds::seeded(9).next_id(),
            &retry(),
        )
        .await
        .unwrap();
        register(
            &store,
            &cluster(),
            &profile(),
            &mut ProposalIds::seeded(1),
            &retry(),
        )
        .await
        .unwrap();
        store
    }

    async fn generation(store: &MemoryControlStore) -> u64 {
        read_cluster(store, &cluster(), &retry())
            .await
            .unwrap()
            .value
            .generation
            .get()
    }

    #[tokio::test]
    async fn a_drain_is_marked_announced_kept_by_the_node_and_cleared_by_an_operator() {
        let store = registered().await;
        let node = profile().node;
        let mut proposals = ProposalIds::seeded(2);
        let before = generation(&store).await;
        let drained = set_draining(&store, &cluster(), &node, true, &mut proposals, &retry())
            .await
            .unwrap();
        assert!(drained.changed && drained.registration.draining);
        assert!(drained.registration.is_leaving());
        assert_eq!(drained.generation.map(Generation::get), Some(before + 1));
        let stored = read(&store, &TypedKey::node(&node)).await.unwrap().unwrap();
        assert!(stored.value.draining && !stored.value.departing);

        // Asking again changes nothing.
        let again = set_draining(&store, &cluster(), &node, true, &mut proposals, &retry())
            .await
            .unwrap();
        assert!(!again.changed && again.generation.is_none());
        assert_eq!(generation(&store).await, before + 1);

        // The node registers again, as at a restart, and keeps the mark;
        // a change of its address keeps it too.
        let unchanged = register(&store, &cluster(), &profile(), &mut proposals, &retry())
            .await
            .unwrap();
        assert_eq!(unchanged.registration, Registration::Unchanged);
        let mut moved = profile();
        moved.address = "10.0.0.9:7400".parse().unwrap();
        let updated = register(&store, &cluster(), &moved, &mut proposals, &retry())
            .await
            .unwrap();
        assert_eq!(updated.registration, Registration::Updated);
        let stored = read(&store, &TypedKey::node(&node)).await.unwrap().unwrap();
        assert!(stored.value.draining);
        assert_eq!(stored.value.address, moved.address);

        // An operator clears it.
        let cleared = set_draining(&store, &cluster(), &node, false, &mut proposals, &retry())
            .await
            .unwrap();
        assert!(cleared.changed && !cleared.registration.draining);
        let stored = read(&store, &TypedKey::node(&node)).await.unwrap().unwrap();
        assert!(!stored.value.draining);
    }

    #[tokio::test]
    async fn only_a_registered_node_is_drained() {
        let store = registered().await;
        let stranger = NodeId::new("node-9").unwrap();
        let refused = set_draining(
            &store,
            &cluster(),
            &stranger,
            true,
            &mut ProposalIds::seeded(2),
            &retry(),
        )
        .await
        .unwrap_err();
        assert!(matches!(refused, DrainError::Unregistered(node) if node == stranger));
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_whose_answer_was_lost_is_settled_and_announced() {
        let store = FaultyStore::new(registered().await);
        let before = generation(store.inner()).await;
        // The read passes; the mark lands with its answer lost, and the
        // store stops answering for the rest of the attempts. Settling,
        // once it answers again, learns that the mark landed.
        let mut faults = vec![Fault::Pass, Fault::LoseResponse];
        faults.extend(std::iter::repeat_n(Fault::Unavailable, 3));
        store.script(faults);
        let node = profile().node;
        let mut proposals = ProposalIds::seeded(2);
        let drained = set_draining(&store, &cluster(), &node, true, &mut proposals, &retry())
            .await
            .unwrap();
        assert!(drained.changed);
        assert_eq!(
            drained.generation.map(Generation::get),
            Some(generation(store.inner()).await)
        );
        assert!(generation(store.inner()).await > before);
        let stored = read(store.inner(), &TypedKey::node(&node))
            .await
            .unwrap()
            .unwrap();
        assert!(stored.value.draining);

        // Settling gets no answer either: the outcome is unknown.
        let mut faults = vec![Fault::Pass, Fault::LoseResponse];
        faults.extend(std::iter::repeat_n(Fault::Unavailable, 7));
        store.script(faults);
        let unknown = set_draining(&store, &cluster(), &node, false, &mut proposals, &retry())
            .await
            .unwrap_err();
        assert!(matches!(unknown, DrainError::Unsettled(_)), "{unknown}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_mark_that_loses_its_race_is_decided_again() {
        let inner = registered().await;
        let store = FaultyStore::new(inner.clone());
        let node = profile().node;
        // Before the mark is written, the node registers again from
        // another address.
        let (racing, mut moved) = (inner.clone(), profile());
        moved.address = "10.0.0.9:7400".parse().unwrap();
        store.script([
            Fault::Pass,
            Fault::before(move || {
                let (store, moved) = (racing.clone(), moved.clone());
                async move {
                    register(
                        &store,
                        &cluster(),
                        &moved,
                        &mut ProposalIds::seeded(5),
                        &retry(),
                    )
                    .await
                    .unwrap();
                }
            }),
        ]);
        let drained = set_draining(
            &store,
            &cluster(),
            &node,
            true,
            &mut ProposalIds::seeded(2),
            &retry(),
        )
        .await
        .unwrap();
        assert!(drained.changed);
        let stored = read(&inner, &TypedKey::node(&node)).await.unwrap().unwrap();
        assert!(stored.value.draining);
        assert_eq!(stored.value.address.to_string(), "10.0.0.9:7400");
    }
}
