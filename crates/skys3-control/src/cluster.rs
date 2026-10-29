//! Cluster bootstrap and the configuration generation in `cluster.json`
//! (design §6.1, §6.2).

use skys3_types::{ClusterDocument, ClusterId, Generation, ProposalId};

use crate::key::{RegisterKey, TypedKey};
use crate::propose::{
    ProposalIds, ProposalOutcome, RetryPolicy, propose_document, read_with_retries,
};
use crate::store::{ControlError, ControlStore, Expected, Versioned};

/// The generation `cluster.json` is created with. A node with no local copy
/// of the control state opens its change stream after
/// [`Generation::ZERO`], so its first report is a snapshot.
pub const FIRST_GENERATION: Generation = Generation::new(1);

/// What [`bootstrap`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bootstrap {
    /// This proposal created `cluster.json`.
    Created(Versioned<ClusterDocument>),
    /// `cluster.json` already existed for this cluster.
    Existing(Versioned<ClusterDocument>),
}

impl Bootstrap {
    /// `cluster.json` as stored.
    #[must_use]
    pub fn cluster(&self) -> &Versioned<ClusterDocument> {
        match self {
            Self::Created(cluster) | Self::Existing(cluster) => cluster,
        }
    }
}

/// Creates `cluster.json` for `cluster_id` with `If-None-Match: *`, or
/// reads the one that exists.
///
/// Any number of nodes may bootstrap at once: exactly one creates the
/// register, and every other one reads it. A caller that retries after an
/// error passes the same `proposal`, so that a creation whose answer was
/// lost is still reported as [`Bootstrap::Created`].
///
/// # Errors
///
/// [`ControlError::ClusterMismatch`] if `cluster.json` names another
/// cluster, [`ControlError::InvalidRegister`] if it is not a valid
/// document (for example, a format version this build does not support),
/// and the errors of [`propose`](crate::propose).
pub async fn bootstrap<S: ControlStore>(
    store: &S,
    cluster_id: &ClusterId,
    proposal: ProposalId,
    policy: &RetryPolicy,
) -> Result<Bootstrap, ControlError> {
    let key = TypedKey::cluster();
    let document = ClusterDocument {
        cluster_id: cluster_id.clone(),
        format_version: ClusterDocument::FORMAT_VERSION,
        generation: FIRST_GENERATION,
        proposal_id: proposal,
    };
    match propose_document(store, &key, Expected::Absent, &document, policy).await? {
        ProposalOutcome::Accepted(version) => Ok(Bootstrap::Created(Versioned {
            value: document,
            version,
        })),
        ProposalOutcome::Rejected => read_cluster(store, cluster_id, policy)
            .await
            .map(Bootstrap::Existing),
    }
}

/// Reads `cluster.json`, retrying under `policy`, and checks that it
/// belongs to `cluster_id`.
///
/// # Errors
///
/// [`ControlError::NotBootstrapped`] if it does not exist,
/// [`ControlError::ClusterMismatch`] if it names another cluster, and the
/// errors of [`read_with_retries`].
pub async fn read_cluster<S: ControlStore>(
    store: &S,
    cluster_id: &ClusterId,
    policy: &RetryPolicy,
) -> Result<Versioned<ClusterDocument>, ControlError> {
    let cluster = read_with_retries(store, &TypedKey::cluster(), policy)
        .await?
        .ok_or(ControlError::NotBootstrapped)?;
    if cluster.value.cluster_id != *cluster_id {
        return Err(ControlError::ClusterMismatch {
            expected: cluster_id.clone(),
            found: cluster.value.cluster_id,
        });
    }
    Ok(cluster)
}

/// Increments the generation in `cluster.json` to announce the register
/// writes the caller has made (design §6.2), and returns a generation that
/// announces them.
///
/// Call it after the writes it announces have been accepted: a change
/// stream that sees the new generation then lists them. If another writer
/// replaces `cluster.json` first, its write came after the caller's
/// writes too, so the generation it stored announces them and is returned
/// without a second increment.
///
/// # Errors
///
/// The errors of [`read_cluster`] and [`propose`](crate::propose), and
/// [`ControlError::RetriesExhausted`] if `cluster.json` keeps changing
/// without its generation moving, which SkyS3 never does.
pub async fn bump_generation<S: ControlStore>(
    store: &S,
    cluster_id: &ClusterId,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Generation, ControlError> {
    let key = TypedKey::cluster();
    for _ in 0..policy.max_attempts {
        let current = read_cluster(store, cluster_id, policy).await?;
        let generation = current.value.generation;
        let document = ClusterDocument {
            // A counter of increments cannot reach 2^64.
            generation: generation.checked_next().unwrap_or(Generation::MAX),
            proposal_id: proposals.next_id(),
            ..current.value
        };
        let expected = Expected::Version(current.version);
        match propose_document(store, &key, expected, &document, policy).await? {
            ProposalOutcome::Accepted(_) => return Ok(document.generation),
            ProposalOutcome::Rejected => {
                let now = read_cluster(store, cluster_id, policy).await?;
                if now.value.generation > generation {
                    return Ok(now.value.generation);
                }
            }
        }
    }
    Err(ControlError::RetriesExhausted {
        key: RegisterKey::cluster(),
        attempts: policy.max_attempts,
        may_have_applied: false,
        last: Box::new(ControlError::Unavailable(
            "cluster.json changed without its generation moving".to_owned(),
        )),
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::faults::{Fault, FaultyStore};
    use crate::memory::MemoryControlStore;
    use crate::store::PutOutcome;

    fn id(name: &str) -> ClusterId {
        ClusterId::new(name).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_bootstrap_creates_and_later_ones_read() {
        let store = MemoryControlStore::new();
        let policy = RetryPolicy::default();
        let mut ids = ProposalIds::seeded(0);
        let first = bootstrap(&store, &id("prod"), ids.next_id(), &policy)
            .await
            .unwrap();
        assert!(matches!(first, Bootstrap::Created(_)));
        assert_eq!(first.cluster().value.generation, FIRST_GENERATION);
        let second = bootstrap(&store, &id("prod"), ids.next_id(), &policy)
            .await
            .unwrap();
        assert_eq!(second, Bootstrap::Existing(first.cluster().clone()));
        let error = bootstrap(&store, &id("other"), ids.next_id(), &policy)
            .await
            .unwrap_err();
        assert!(
            matches!(error, ControlError::ClusterMismatch { .. }),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            "the control store belongs to cluster prod, not other"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_bootstrap_whose_answer_was_lost_still_created_the_cluster() {
        let store = FaultyStore::new(MemoryControlStore::new());
        store.script([Fault::LoseResponse]);
        let outcome = bootstrap(
            &store,
            &id("prod"),
            ProposalIds::seeded(1).next_id(),
            &RetryPolicy::default(),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, Bootstrap::Created(_)), "{outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn generations_count_every_increment() {
        let store = MemoryControlStore::new();
        let policy = RetryPolicy::default();
        let mut ids = ProposalIds::seeded(2);
        let error = bump_generation(&store, &id("prod"), &mut ids, &policy)
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::NotBootstrapped), "{error}");
        bootstrap(&store, &id("prod"), ids.next_id(), &policy)
            .await
            .unwrap();
        for expected in 2..=4 {
            let generation = bump_generation(&store, &id("prod"), &mut ids, &policy)
                .await
                .unwrap();
            assert_eq!(generation, Generation::new(expected));
        }
        let cluster = read_cluster(&store, &id("prod"), &policy).await.unwrap();
        assert_eq!(cluster.value.generation, Generation::new(4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_racing_increment_announces_the_callers_writes() {
        let memory = MemoryControlStore::new();
        let store = FaultyStore::new(memory.clone());
        let policy = RetryPolicy::default();
        let mut ids = ProposalIds::seeded(3);
        bootstrap(&store, &id("prod"), ids.next_id(), &policy)
            .await
            .unwrap();
        // Another coordinator increments between our read and our write.
        let other = memory.clone();
        store.script([
            Fault::Pass,
            Fault::before(move || {
                let other = other.clone();
                async move {
                    let mut ids = ProposalIds::seeded(4);
                    let policy = RetryPolicy::default();
                    let bumped = bump_generation(&other, &id("prod"), &mut ids, &policy).await;
                    assert_eq!(bumped.unwrap(), Generation::new(2));
                }
            }),
        ]);
        let generation = bump_generation(&store, &id("prod"), &mut ids, &policy)
            .await
            .unwrap();
        assert_eq!(generation, Generation::new(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_rewrite_that_keeps_the_generation_is_not_an_increment() {
        let memory = MemoryControlStore::new();
        let store = FaultyStore::new(memory.clone());
        let policy = RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        };
        let mut ids = ProposalIds::seeded(5);
        bootstrap(&store, &id("prod"), ids.next_id(), &policy)
            .await
            .unwrap();
        // Something other than SkyS3 rewrites cluster.json before each of
        // our writes without moving the generation.
        let rewrite = move || {
            let other = memory.clone();
            async move {
                let current = other.get(&RegisterKey::cluster()).await.unwrap().unwrap();
                let mut document = crate::propose::read(&other, &TypedKey::cluster())
                    .await
                    .unwrap()
                    .unwrap()
                    .value;
                document.proposal_id = ProposalIds::seeded(6).next_id();
                let value = Bytes::from(skys3_types::RegisterDocument::to_json(&document).unwrap());
                let written = other
                    .put_if(
                        &RegisterKey::cluster(),
                        Expected::Version(current.version),
                        value,
                    )
                    .await
                    .unwrap();
                assert!(matches!(written, PutOutcome::Written(_)));
            }
        };
        store.script([
            Fault::Pass,
            Fault::before(rewrite.clone()),
            Fault::Pass,
            Fault::Pass,
            Fault::before(rewrite),
        ]);
        let error = bump_generation(&store, &id("prod"), &mut ids, &policy)
            .await
            .unwrap_err();
        assert!(
            matches!(error, ControlError::RetriesExhausted { .. }),
            "{error}"
        );
    }
}
