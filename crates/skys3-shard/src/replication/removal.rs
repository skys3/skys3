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
use skys3_log::ShardRef;
use skys3_types::{NodeId, ProposalId, Seq, ShardConfig};
use tokio::time::Instant;

use super::{Backfill, ReplicationConfig};
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

/// The shard registers of the control store, as replicas change their
/// shard's configuration through them: a primary removing members, and a
/// member taking over (§6.1, §6.3).
pub trait ShardRegisters: Send + Sync + 'static {
    /// Replaces `current` with `next` in the shard's register, if the
    /// register still holds `current`, with the lost-response rule of
    /// §6.1.
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>>;

    /// The configuration the register of `shard` holds, or `None` if it
    /// holds none.
    fn read<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>>;
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

    fn read<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        Box::pin(async move {
            let key = TypedKey::shard(&shard.bucket, shard.shard);
            let held = read_with_retries(&self.store, &key, &self.policy).await?;
            Ok(held.map(|held| held.value))
        })
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
    pub(super) fn next_id(&self) -> ProposalId {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_id()
    }
}

/// What the watchdog last saw of a member.
#[derive(Debug, Clone, Copy)]
pub(super) struct Seen {
    pub(super) acked: Seq,
    pub(super) heard: u64,
    /// When the member last responded.
    responded: Instant,
}

impl Seen {
    /// How long the member or learner has not responded, at `now`.
    pub(super) fn silent(&self, now: Instant) -> Duration {
        now.duration_since(self.responded)
    }
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
/// (§6.5). So does a member's refusal of an append for a newer epoch, once
/// the register confirms it: the watchdog reads the register then.
///
/// The watchdog also tends the primary's learners, by the same measure of
/// responding, and promotes them (§6.4, §6.7; see the `learners` module).
pub(super) struct Watchdog<D: Disk> {
    pub shard: Shard<D>,
    pub leader: Arc<Leader>,
    pub node: NodeId,
    pub removal: Arc<Removal>,
    pub config: ReplicationConfig,
    /// Called with each configuration the primary adopts.
    pub adopted: Box<dyn Fn(&ShardConfig) + Send + Sync>,
    /// What backfills the learners, if anything beyond the live stream
    /// does.
    pub backfill: Option<Arc<dyn Backfill>>,
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
        let mut rejections = self.leader.rejections();
        let backfilling = Arc::default();
        while !self.shard.is_stopped() {
            tokio::select! {
                biased;
                changed = rejections.changed() => {
                    // A handoff in progress reads the register itself.
                    if changed.is_ok() && let Ok(_changing) = self.leader.changing().try_lock() {
                        self.check_register().await;
                    }
                    continue;
                }
                () = tokio::time::sleep(interval) => {}
            }
            let now = Instant::now();
            let suspects: Vec<NodeId> = self
                .leader
                .members()
                .into_iter()
                .filter(|member| {
                    self.observe(&mut seen, member, now, sequenced)
                        .is_some_and(|seen| seen.silent(now) >= self.config.member_suspect_after)
                })
                .collect();
            self.tend_learners(&mut seen, now, sequenced, &backfilling);
            sequenced = self.shard.last_sequenced();
            // A primary that steps down removes and promotes no one: the
            // candidate proposes over the configuration it stepped down in
            // (§5.4).
            if self.shard.stepped_down().is_some() {
                continue;
            }
            if let Ok(_changing) = self.leader.changing().try_lock() {
                if suspects.is_empty() {
                    self.promote().await;
                } else {
                    self.remove(&suspects).await;
                }
            }
        }
    }

    /// Updates what the watchdog saw of `node`, a member or a learner, at
    /// `now`, when the primary had sequenced up to `sequenced` a check ago,
    /// and returns it, unless the primary does not link to `node`.
    pub(super) fn observe<'a>(
        &self,
        seen: &'a mut BTreeMap<NodeId, Seen>,
        node: &NodeId,
        now: Instant,
        sequenced: Seq,
    ) -> Option<&'a Seen> {
        let (acked, heard) = self.leader.heard(node)?;
        let seen = seen.entry(node.clone()).or_insert(Seen {
            acked,
            heard,
            responded: now,
        });
        if acked > seen.acked || (heard > seen.heard && acked >= sequenced) {
            seen.responded = now;
        }
        seen.acked = acked;
        seen.heard = heard;
        Some(seen)
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
            Ok(Replaced::Holds(held)) => self.follow(&current, held.as_ref()),
            Err(error) => {
                tracing::warn!(%shard, %error, "removing members failed; trying again");
            }
        }
    }

    /// Reads the shard's register after a member refused an append for a
    /// newer epoch, and follows what it holds (§6.5). A failed read is
    /// tried again on the next refusal.
    async fn check_register(&self) {
        let shard = self.shard.shard();
        match self.removal.registers.read(shard).await {
            Ok(held) => self.follow(&self.shard.config(), held.as_ref()),
            Err(error) => {
                tracing::warn!(%shard, %error, "reading the shard's register failed");
            }
        }
    }

    /// Acts on `held`, what the shard's register holds while this primary
    /// is in `current`: adopts a change that keeps it primary and adds no
    /// member that was not a learner (the coordinator's removals and
    /// learners, or an earlier proposal of this primary that landed
    /// unseen), and stops as deposed if the register names another primary
    /// or is gone. It then redirects to the register's configuration.
    pub(super) fn follow(&self, current: &ShardConfig, held: Option<&ShardConfig>) {
        let shard = self.shard.shard();
        match held {
            Some(held) if self.can_adopt(current, held) => self.adopt(held),
            Some(held) if held.epoch <= current.epoch => {
                tracing::warn!(%shard, ?held, "the shard's register is behind this primary");
            }
            held => {
                tracing::warn!(%shard, ?held, "the shard's register names another configuration");
                self.shard.depose(
                    "the shard's register names a configuration this primary cannot adopt",
                    held,
                );
            }
        }
    }

    /// Whether the primary can adopt `held` over `current`: a newer epoch
    /// with the same primary, whose members were members or learners of
    /// `current`. A learner made a member by a promotion the primary did
    /// not propose holds up commits from then on, as a member does.
    fn can_adopt(&self, current: &ShardConfig, held: &ShardConfig) -> bool {
        held.epoch > current.epoch
            && held.primary == self.node
            && held
                .members
                .iter()
                .all(|member| current.is_member(member) || current.is_learner(member))
    }

    /// Adopts `config` without waiting for its `CONFIG` record to commit,
    /// which another late member may hold up.
    pub(super) fn adopt(&self, config: &ShardConfig) {
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
