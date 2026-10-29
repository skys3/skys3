//! Member removal (§6.3, §6.4): a primary watches whether its members
//! respond, and removes one that stays unresponsive for
//! `member_suspect_after` by replacing its shard's register with the next
//! configuration, compare-and-swap over the current one.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_control::{
    ControlError, ControlStore, Expected, ProposalIds, ProposalOutcome, RetryPolicy, TypedKey,
    propose_document, read_with_retries,
};
use skys3_io::Disk;
use skys3_types::{NodeId, ProposalId, Seq, ShardConfig};
use tokio::time::Instant;

use super::ReplicationConfig;
use crate::leader::Leader;
use crate::shard::Shard;

/// A boxed future that is `Send`, as the trait objects here return.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a shard register held when a primary tried to replace it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replaced {
    /// The register holds the new configuration: this proposal's, written
    /// now or by an earlier attempt whose answer was lost.
    Accepted,
    /// The register held another value, which it keeps: a configuration
    /// another proposer wrote, or `None` if the register is gone.
    Holds(Option<ShardConfig>),
}

/// The shard registers of the control store, as a primary changes its
/// shard's configuration through them (§6.1, §6.3).
pub trait ShardRegisters: Send + Sync + 'static {
    /// Replaces `current` with `next` in the shard's register, if the
    /// register still holds `current`, with the lost-response rule of
    /// §6.1.
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>>;
}

/// [`ShardRegisters`] in any [`ControlStore`], under the register layout
/// of §6.1 (`shards/<bucket-id>/<n>.json`).
#[derive(Debug, Clone)]
pub struct ControlRegisters<S> {
    store: S,
    policy: RetryPolicy,
}

impl<S: ControlStore> ControlRegisters<S> {
    /// The shard registers of `store`, whose requests retry under
    /// `policy`.
    #[must_use]
    pub fn new(store: S, policy: RetryPolicy) -> Self {
        Self { store, policy }
    }

    async fn replace_now(
        &self,
        current: &ShardConfig,
        next: &ShardConfig,
    ) -> Result<Replaced, ControlError> {
        let key = TypedKey::shard(&current.bucket_id, current.shard);
        let Some(held) = read_with_retries(&self.store, &key, &self.policy).await? else {
            return Ok(Replaced::Holds(None));
        };
        // An earlier attempt landed although its answer was lost.
        if held.value == *next {
            return Ok(Replaced::Accepted);
        }
        if held.value != *current {
            return Ok(Replaced::Holds(Some(held.value)));
        }
        let expected = Expected::Version(held.version);
        match propose_document(&self.store, &key, expected, next, &self.policy).await? {
            ProposalOutcome::Accepted(_) => Ok(Replaced::Accepted),
            ProposalOutcome::Rejected => {
                let held = read_with_retries(&self.store, &key, &self.policy).await?;
                Ok(Replaced::Holds(held.map(|held| held.value)))
            }
        }
    }
}

impl<S: ControlStore> ShardRegisters for ControlRegisters<S> {
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>> {
        Box::pin(self.replace_now(current, next))
    }
}

/// What a node's primaries remove members through: the shard registers,
/// and fresh proposal IDs.
pub(super) struct Removal {
    pub registers: Box<dyn ShardRegisters>,
    pub ids: Mutex<ProposalIds>,
}

impl fmt::Debug for Removal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Removal").finish_non_exhaustive()
    }
}

impl Removal {
    fn next_id(&self) -> ProposalId {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_id()
    }
}

/// What the watchdog last saw of a member.
#[derive(Debug, Clone, Copy)]
struct Seen {
    acked: Seq,
    heard: u64,
    /// When the member last responded.
    responded: Instant,
}

/// A primary's watch over its members (§6.4).
///
/// A member **responds** while its acknowledgements advance, or, once it
/// holds every record the primary had sequenced a moment ago, while it
/// answers at all: an idle member answers every beacon, and a busy one
/// acknowledges the records it makes durable. A member that answers
/// nothing, or only beacons while a record it lacks waits, does not. One
/// that does not respond for `member_suspect_after` is removed: the
/// primary replaces the shard's register with the next epoch, without the
/// member, and adopts it. Records the member held back then commit under
/// the new epoch, as soon as the primary's `CONFIG` record is durable
/// ([`Shard::reconfigure`]).
///
/// A failed attempt is tried again on a later check. A register that names
/// another primary, or no longer exists, deposes this one: it stops
/// (§6.5).
pub(super) struct Watchdog<D: Disk> {
    pub shard: Shard<D>,
    pub leader: Arc<Leader>,
    pub node: NodeId,
    pub removal: Arc<Removal>,
    pub config: ReplicationConfig,
    /// Called with each configuration the primary adopts.
    pub adopted: Box<dyn Fn(&ShardConfig) + Send + Sync>,
}

impl<D: Disk> Watchdog<D> {
    /// How often the watchdog checks the members.
    fn interval(&self) -> Duration {
        self.config
            .beacon_every(true)
            .min(self.config.member_suspect_after / 4)
            .max(Duration::from_millis(1))
    }

    /// Watches until the shard stops.
    pub(super) async fn run(self) {
        let interval = self.interval();
        let mut seen: BTreeMap<NodeId, Seen> = BTreeMap::new();
        let mut sequenced = self.shard.last_sequenced();
        while !self.shard.is_stopped() {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            let mut suspects = Vec::new();
            for member in self.leader.members() {
                let Some((acked, heard)) = self.leader.heard(&member) else {
                    continue;
                };
                let seen = seen.entry(member.clone()).or_insert(Seen {
                    acked,
                    heard,
                    responded: now,
                });
                if acked > seen.acked || (heard > seen.heard && acked >= sequenced) {
                    seen.responded = now;
                }
                seen.acked = acked;
                seen.heard = heard;
                if now.duration_since(seen.responded) >= self.config.member_suspect_after {
                    suspects.push(member);
                }
            }
            sequenced = self.shard.last_sequenced();
            if !suspects.is_empty() {
                self.remove(&suspects).await;
            }
        }
    }

    /// Replaces the shard's register with the next configuration, without
    /// `suspects`, and adopts what the register then holds, if it can.
    async fn remove(&self, suspects: &[NodeId]) {
        let shard = self.shard.shard();
        let current = self.shard.config();
        let members: Vec<NodeId> = current
            .members
            .iter()
            .filter(|member| !suspects.contains(member))
            .cloned()
            .collect();
        let Some(epoch) = current.epoch.checked_next() else {
            return;
        };
        if members.len() == current.members.len() {
            return;
        }
        let next = ShardConfig {
            epoch,
            members,
            proposal_id: self.removal.next_id(),
            ..current.clone()
        };
        tracing::info!(%shard, ?suspects, %epoch, "removing unresponsive members");
        match self.removal.registers.replace(&current, &next).await {
            Ok(Replaced::Accepted) => self.adopt(&next),
            // The coordinator removed members, or an earlier proposal of
            // this primary landed unseen.
            Ok(Replaced::Holds(Some(held))) if self.can_adopt(&current, &held) => {
                self.adopt(&held);
            }
            Ok(Replaced::Holds(held)) if held.as_ref().is_none_or(|h| h.epoch > current.epoch) => {
                tracing::warn!(%shard, ?held, "the shard's register names another configuration");
                self.shard
                    .depose("the shard's register names a configuration this primary cannot adopt");
            }
            Ok(Replaced::Holds(held)) => {
                tracing::warn!(%shard, ?held, "the shard's register is behind this primary");
            }
            Err(error) => {
                tracing::warn!(%shard, %error, "removing members failed; trying again");
            }
        }
    }

    /// Whether the primary can adopt `held` over `current`: a newer epoch
    /// with the same primary, and only members of `current`.
    fn can_adopt(&self, current: &ShardConfig, held: &ShardConfig) -> bool {
        held.epoch > current.epoch
            && held.primary == self.node
            && held.learners.is_empty()
            && held.members.iter().all(|member| current.is_member(member))
    }

    /// Adopts `config` without waiting for its `CONFIG` record to commit,
    /// which another late member may hold up.
    fn adopt(&self, config: &ShardConfig) {
        match self.shard.begin_reconfigure(config) {
            Ok(reply) => {
                drop(reply);
                (self.adopted)(config);
            }
            Err(error) => {
                tracing::warn!(shard = %self.shard.shard(), %error, "adopting a configuration failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use skys3_control::{MemoryControlStore, ProposalIds};
    use skys3_types::{BucketId, Epoch, ProposalId, ShardId};

    use super::*;

    fn node(n: u8) -> NodeId {
        format!("node-{n}").parse().unwrap()
    }

    fn config(epoch: u64, members: &[u8], id: &str) -> ShardConfig {
        ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
            epoch: Epoch::new(epoch),
            primary: node(1),
            members: members.iter().map(|n| node(*n)).collect(),
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 3,
            proposal_id: ProposalId::new(id).unwrap(),
        }
    }

    #[tokio::test]
    async fn registers_are_replaced_only_over_the_current_configuration() {
        let store = MemoryControlStore::new();
        let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
        let first = config(1, &[1, 2, 3], "a");
        let second = config(2, &[1, 2], "b");
        // No register yet.
        assert_eq!(
            registers.replace(&first, &second).await.unwrap(),
            Replaced::Holds(None)
        );
        let key = TypedKey::shard(&first.bucket_id, first.shard);
        let created = propose_document(
            &store,
            &key,
            Expected::Absent,
            &first,
            &RetryPolicy::default(),
        )
        .await
        .unwrap();
        assert!(matches!(created, ProposalOutcome::Accepted(_)));
        assert_eq!(
            registers.replace(&first, &second).await.unwrap(),
            Replaced::Accepted
        );
        // Again, as after a lost answer: the register holds the proposal.
        assert_eq!(
            registers.replace(&first, &second).await.unwrap(),
            Replaced::Accepted
        );
        // A competing proposal over the same configuration finds the winner.
        let other = config(2, &[1, 3], "c");
        assert_eq!(
            registers.replace(&first, &other).await.unwrap(),
            Replaced::Holds(Some(second.clone()))
        );
        let ids = ProposalIds::seeded(1);
        let removal = Removal {
            registers: Box::new(registers),
            ids: Mutex::new(ids),
        };
        assert_ne!(removal.next_id(), removal.next_id());
        assert!(format!("{removal:?}").contains("Removal"));
    }
}
