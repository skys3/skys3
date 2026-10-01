//! A node's registration in `nodes/<node-id>.json` (design §6.1, §6.7):
//! what makes a node part of the cluster.
//!
//! A node registers itself when it starts, with no operator action: its
//! credentials name it, and its configuration gives its address, labels,
//! and disks. The first start creates the register with `If-None-Match: *`.
//! A restart rewrites it with `If-Match` only if something changed, so a
//! node that restarts as it was costs one read. Every write is announced by
//! a generation increment through the coordinator's change path
//! ([`apply`]), so other nodes and the coordinator find the registration
//! when they next list `nodes/`.

use skys3_control::{ControlError, ControlStore, ProposalIds, RetryPolicy, TypedKey, Version};
use skys3_types::{
    ClusterId, DiskInfo, Generation, Label, NodeAddress, NodeId, NodeRegistration, ProposalId,
};

use crate::change::{ChangeError, ChangeSet, apply};

/// How many times [`register`] re-reads a register another writer changed
/// under it before it gives up.
const MAX_ROUNDS: usize = 8;

/// What a node registers about itself: everything in its
/// [`NodeRegistration`] except the `proposal_id` of the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeProfile {
    /// The node's ID, which its transport certificate names.
    pub node: NodeId,
    /// The `host:port` of its intra-cluster transport.
    pub address: NodeAddress,
    /// The node's zone label, if it has one.
    pub zone: Option<Label>,
    /// The node's rack label, if it has one.
    pub rack: Option<Label>,
    /// The disks the node offers.
    pub disks: Vec<DiskInfo>,
}

impl NodeProfile {
    /// The registration document for this profile, stored by the write
    /// that carries `proposal`.
    #[must_use]
    pub fn document(&self, proposal: ProposalId) -> NodeRegistration {
        NodeRegistration {
            node_id: self.node.clone(),
            address: self.address.clone(),
            zone: self.zone.clone(),
            rack: self.rack.clone(),
            disks: self.disks.clone(),
            proposal_id: proposal,
        }
    }

    /// Whether `registration` already says everything this profile does.
    #[must_use]
    pub fn describes(&self, registration: &NodeRegistration) -> bool {
        registration.node_id == self.node
            && registration.address == self.address
            && registration.zone == self.zone
            && registration.rack == self.rack
            && registration.disks == self.disks
    }
}

/// What [`register`] did to the node's register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    /// The node was not registered, and now is: its first start, or its
    /// first since the coordinator forgot it.
    Created,
    /// The register described the node differently, and now describes it
    /// as it is.
    Updated,
    /// The register already described the node as it is, and was left
    /// alone.
    Unchanged,
}

/// The outcome of a successful [`register`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// What happened to the register.
    pub registration: Registration,
    /// The version the register holds the node's registration at.
    pub version: Version,
    /// The generation that announces the write, if there was one.
    pub generation: Option<Generation>,
}

/// Why a node could not register.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegistrationError {
    /// The control store failed, or holds a register this build cannot
    /// read, which is never overwritten unseen.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// The profile makes an invalid registration, such as one that lists a
    /// disk twice.
    #[error(transparent)]
    Invalid(#[from] ChangeError),
    /// Another writer kept changing the register: most likely a second
    /// process running with the same node ID.
    #[error("the registration of {0} kept changing under this node")]
    Contended(NodeId),
}

/// Registers the node `profile` describes in `nodes/<node-id>.json`:
/// creates the register if it is absent, rewrites it under `If-Match` if
/// it describes the node differently, and leaves it alone otherwise. A
/// write is announced by a generation increment in `cluster.json`.
///
/// A write that loses its compare-and-swap, to a coordinator forgetting
/// the node or to anything else, is planned again from a fresh read. One
/// whose answer was lost is resolved by the lost-response rule, and one
/// that may have landed is announced all the same ([`apply`]).
///
/// # Errors
///
/// [`RegistrationError`]. The caller tries again later.
pub async fn register<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    profile: &NodeProfile,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Registered, RegistrationError> {
    let key = TypedKey::node(&profile.node);
    for _ in 0..MAX_ROUNDS {
        let current = skys3_control::read_with_retries(store, &key, policy).await?;
        let document = profile.document(proposals.next_id());
        let (change, registration) = match current {
            Some(current) if profile.describes(&current.value) => {
                return Ok(Registered {
                    registration: Registration::Unchanged,
                    version: current.version,
                    generation: None,
                });
            }
            Some(current) => (
                ChangeSet::new().update(&key, &current.version, &document)?,
                Registration::Updated,
            ),
            None => (
                ChangeSet::new().create(&key, &document)?,
                Registration::Created,
            ),
        };
        let applied = apply(store, cluster, &change, proposals, policy).await?;
        if let Some((_, Some(version))) = applied.written.into_iter().next() {
            tracing::info!(node = %profile.node, ?registration, "registered the node");
            return Ok(Registered {
                registration,
                version,
                generation: applied.generation,
            });
        }
        tracing::debug!(node = %profile.node, "the registration changed under this node; reading it again");
    }
    Err(RegistrationError::Contended(profile.node.clone()))
}

#[cfg(test)]
mod tests {
    use skys3_control::faults::{Fault, FaultyStore};
    use skys3_control::{Expected, MemoryControlStore, bootstrap, read, read_cluster};
    use skys3_types::RegisterDocument;

    use super::*;

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

    fn profile(n: u8) -> NodeProfile {
        NodeProfile {
            node: NodeId::new(format!("node-{n}")).unwrap(),
            address: format!("10.0.0.{n}:7400").parse().unwrap(),
            zone: Some(Label::new("zone-a").unwrap()),
            rack: Some(Label::new(format!("rack-{n}")).unwrap()),
            disks: vec![DiskInfo {
                disk_id: Label::new("nvme0").unwrap(),
                capacity_bytes: 1 << 40,
            }],
        }
    }

    async fn store() -> MemoryControlStore {
        let store = MemoryControlStore::new();
        let mut ids = ProposalIds::seeded(7);
        bootstrap(&store, &cluster(), ids.next_id(), &retry())
            .await
            .unwrap();
        store
    }

    async fn generation(store: &MemoryControlStore) -> Generation {
        read_cluster(store, &cluster(), &retry())
            .await
            .unwrap()
            .value
            .generation
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_start_creates_and_a_restart_changes_only_what_changed() {
        let store = store().await;
        let mut ids = ProposalIds::seeded(1);
        let mut node = profile(1);
        let first = register(&store, &cluster(), &node, &mut ids, &retry())
            .await
            .unwrap();
        assert_eq!(first.registration, Registration::Created);
        assert_eq!(first.generation, Some(generation(&store).await));
        let stored = read(&store, &TypedKey::node(&node.node))
            .await
            .unwrap()
            .unwrap();
        assert!(node.describes(&stored.value));
        assert_eq!(stored.version, first.version);

        // A restart as before reads, and writes and announces nothing.
        let before = generation(&store).await;
        let again = register(&store, &cluster(), &node, &mut ids, &retry())
            .await
            .unwrap();
        assert_eq!(again.registration, Registration::Unchanged);
        assert_eq!(
            (again.version, again.generation),
            (first.version.clone(), None)
        );
        assert_eq!(generation(&store).await, before);

        // A restart with another address rewrites the register.
        node.address = "10.0.1.1:7400".parse().unwrap();
        let moved = register(&store, &cluster(), &node, &mut ids, &retry())
            .await
            .unwrap();
        assert_eq!(moved.registration, Registration::Updated);
        assert_ne!(moved.version, first.version);
        assert!(moved.generation.unwrap() > before);
        let stored = read(&store, &TypedKey::node(&node.node))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.value.address, node.address);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_answer_still_registers_and_announces() {
        let store = FaultyStore::new(store().await);
        let mut ids = ProposalIds::seeded(2);
        // The read passes, and the create lands but its answer is lost.
        store.script([Fault::Pass, Fault::LoseResponse]);
        let registered = register(&store, &cluster(), &profile(2), &mut ids, &retry())
            .await
            .unwrap();
        assert_eq!(registered.registration, Registration::Created);
        assert_eq!(registered.generation, Some(generation(store.inner()).await));
    }

    /// A fault hook that rewrites `node`'s register, as another writer
    /// would, before the request it is scripted for.
    fn interfere(store: &MemoryControlStore, node: &NodeProfile) -> Fault {
        let (store, mut other) = (store.clone(), node.clone());
        other.rack = Some(Label::new("elsewhere").unwrap());
        Fault::before(move || {
            let (store, other) = (store.clone(), other.clone());
            async move {
                let key = TypedKey::node(&other.node);
                let expected = match read(&store, &key).await.unwrap() {
                    Some(current) => Expected::Version(current.version),
                    None => Expected::Absent,
                };
                let document = other.document(ProposalIds::from_os_rng().next_id());
                let _ = store
                    .put_if(key.key(), expected, document.to_json().unwrap().into())
                    .await
                    .unwrap();
            }
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_loses_its_race_is_planned_again() {
        let inner = store().await;
        let store = FaultyStore::new(inner.clone());
        let node = profile(5);
        // Another writer creates the register between the read and the
        // create, so the create fails, and the node updates what it reads.
        store.script([Fault::Pass, interfere(&inner, &node)]);
        let registered = register(
            &store,
            &cluster(),
            &node,
            &mut ProposalIds::seeded(5),
            &retry(),
        )
        .await
        .unwrap();
        assert_eq!(registered.registration, Registration::Updated);
        let stored = read(&inner, &TypedKey::node(&node.node))
            .await
            .unwrap()
            .unwrap();
        assert!(node.describes(&stored.value));
    }

    #[tokio::test(start_paused = true)]
    async fn a_register_that_keeps_changing_is_reported() {
        let inner = store().await;
        let store = FaultyStore::new(inner.clone());
        let node = profile(6);
        for _ in 0..MAX_ROUNDS {
            store.script([Fault::Pass, interfere(&inner, &node)]);
        }
        let error = register(
            &store,
            &cluster(),
            &node,
            &mut ProposalIds::seeded(6),
            &retry(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, RegistrationError::Contended(id) if *id == node.node),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreadable_register_is_never_overwritten() {
        let store = store().await;
        let node = profile(3);
        let key = TypedKey::node(&node.node);
        let _ = store
            .put_if(
                key.key(),
                Expected::Absent,
                bytes::Bytes::from_static(b"{\"from\":\"a newer format\"}"),
            )
            .await
            .unwrap();
        let error = register(
            &store,
            &cluster(),
            &node,
            &mut ProposalIds::seeded(3),
            &retry(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                RegistrationError::Control(ControlError::InvalidRegister { .. })
            ),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalid_profile_is_refused() {
        let store = store().await;
        let mut node = profile(4);
        node.disks.push(node.disks[0].clone());
        let error = register(
            &store,
            &cluster(),
            &node,
            &mut ProposalIds::seeded(4),
            &retry(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, RegistrationError::Invalid(_)), "{error}");
    }
}
