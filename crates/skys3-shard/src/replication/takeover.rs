//! Primary takeover (§5.4, §6.5): a member whose primary has gone silent
//! proposes itself as the shard's primary by a compare-and-swap of the
//! shard's register, and reconciles the members with its log.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use skys3_io::Disk;
use skys3_net::Network;
use skys3_types::ShardConfig;

use super::Replication;
use super::removal::{Removal, Replaced};
use crate::lease::Grace;
use crate::shard::{Role, Shard};

/// How many records past its applied position a candidate needs to wait
/// the shortest delay.
const LONGEST_LEAD: u32 = 8;

/// A member's watch over its primary, ready to take over (§6.5), and to
/// take a planned handoff (§5.4).
///
/// Once `primary_grace` has passed since the member last granted the
/// primary a lease, every lease it granted has expired, so the old primary
/// serves no read on it (§5.4). The member waits a short delay that
/// shrinks as it holds more records past what it applied, so that the
/// member with the longest log usually proposes first, then stops granting
/// leases and acknowledging appends (R1, §6.3) and proposes the next
/// configuration: itself as primary, the other members, and not the old
/// primary. If the register accepts it, the member becomes the primary
/// ([`Shard::take_over`]), and its links reconcile the members with its log
/// before it serves (§6.6).
///
/// A proposal that loses follows the register: the member grants again,
/// and waits for the winner's session, which brings the new configuration.
/// One that loses to a removal of other members by the same old primary
/// adopts it and proposes again over it at once. Until it learns the
/// outcome, after a lost answer for example, it acts as if it won: it
/// grants nothing and acknowledges nothing, and asks the register again.
/// A node that restarts learns the outcome from the register before it
/// opens the shard again, so the proposal needs no record of its own.
///
/// A planned handoff takes the same path from the compare-and-swap on:
/// the old primary has stepped down durably, so its leases no longer
/// matter, and the member it stepped down to proposes as soon as it holds
/// the old primary's last record ([`Grace::step_down`]). If the step-down
/// message is lost, the member waits out its grace as after silence.
pub(super) struct Candidate<N: Network, D: Disk> {
    pub replication: Replication<N, D>,
    pub shard: Shard<D>,
    pub grace: Arc<Grace>,
    pub removal: Arc<Removal>,
}

/// How a proposal ended.
enum Proposed {
    /// The member took over, or can no longer: the watch ends.
    Done,
    /// The member follows another configuration: it watches again.
    Lost,
    /// The member adopted a removal by the same old primary: it proposes
    /// again at once.
    Again,
}

impl<N: Network, D: Disk> Candidate<N, D> {
    /// Watches until the member takes over, or its replica stops or is no
    /// longer a member. A primary that steps down to this member for a
    /// planned handoff lets it propose at once, without the grace or the
    /// delay (§5.4).
    pub(super) async fn run(self) {
        loop {
            let epoch = self.shard.config().epoch;
            let handed = tokio::select! {
                biased;
                () = self.grace.stepped_down_since(epoch) => true,
                () = self.grace.passed() => false,
            };
            if !self.is_member() {
                return;
            }
            if !handed {
                tokio::time::sleep(self.delay()).await;
            }
            // A grant in between means the primary is back.
            if !self.is_member() || !self.grace.stop_if_passed(self.shard.config().epoch) {
                continue;
            }
            loop {
                match self.propose().await {
                    Proposed::Done => return,
                    Proposed::Lost => break,
                    Proposed::Again => {}
                }
            }
        }
    }

    /// Whether the replica is a running member, which may take over.
    fn is_member(&self) -> bool {
        !self.shard.is_stopped() && self.shard.role() == Role::Member
    }

    /// The delay before proposing: the longest for a member that holds no
    /// record past what it applied, down to an eighth of it for one that
    /// holds [`LONGEST_LEAD`] or more, plus up to a quarter of it drawn
    /// from the node, the shard, and the epoch, so that members with
    /// equal logs propose at different times (§6.5).
    fn delay(&self) -> Duration {
        let spread = self.replication.inner.config.takeover_delay;
        let durable = self.shard.durable().borrow().get();
        let lead = durable.saturating_sub(self.shard.applied().seq.get());
        let lead = u32::try_from(lead)
            .unwrap_or(u32::MAX)
            .min(LONGEST_LEAD - 1);
        let base = spread * (LONGEST_LEAD - lead) / LONGEST_LEAD;
        let mut hasher = DefaultHasher::new();
        let config = self.shard.config();
        (
            &self.replication.inner.node,
            self.shard.shard(),
            config.epoch,
        )
            .hash(&mut hasher);
        let jitter = u32::try_from(hasher.finish() % 1024).unwrap_or(0);
        base + spread / 4 * jitter / 1024
    }

    /// Proposes this member as the primary of the next configuration, and
    /// acts on the outcome.
    async fn propose(&self) -> Proposed {
        let inner = &self.replication.inner;
        let (node, shard) = (&inner.node, self.shard.shard());
        let over = self.shard.config();
        let Some(epoch) = over.epoch.checked_next() else {
            return Proposed::Done;
        };
        if over.primary == *node || !over.is_member(node) {
            self.grace.resume();
            return Proposed::Done;
        }
        let next = ShardConfig {
            epoch,
            primary: node.clone(),
            members: over
                .members
                .iter()
                .filter(|member| **member != over.primary)
                .cloned()
                .collect(),
            learners: Vec::new(),
            proposal_id: self.removal.next_id(),
            ..over.clone()
        };
        tracing::info!(%shard, %epoch, old = %over.primary, "proposing a takeover");
        loop {
            // A session of a newer configuration told the member it lost,
            // and it may have granted since: it proposes nothing until its
            // grace passes again (R1).
            if self.shard.config() != over
                || !self.is_member()
                || !self.grace.propose_over(over.epoch)
            {
                return Proposed::Lost;
            }
            match self.removal.registers.replace(&over, &next).await {
                Ok(Replaced::Accepted) => {
                    return match self.shard.take_over(&over, &next).await {
                        Ok(()) => {
                            tracing::info!(%shard, %epoch, "took over as primary");
                            self.replication.start_primary(&self.shard);
                            Proposed::Done
                        }
                        Err(error) => {
                            tracing::warn!(%shard, %error, "taking over failed");
                            Proposed::Lost
                        }
                    };
                }
                Ok(Replaced::Holds(Some(held))) if held.epoch > over.epoch => {
                    return self.follow(&over, &held).await;
                }
                Ok(Replaced::Holds(held)) => {
                    tracing::warn!(%shard, ?held, "the shard's register is not newer");
                    self.grace.resume();
                    return Proposed::Lost;
                }
                Err(error) => {
                    tracing::warn!(%shard, %error, "proposing a takeover failed; trying again");
                    tokio::time::sleep(inner.config.reconnect_delay).await;
                }
            }
        }
    }

    /// Follows `held`, which the register holds instead of `over`: the
    /// member adopts it, so that it refuses the old primary (rule R2) and,
    /// should the winner go silent before its first session, proposes
    /// over the winner's configuration next.
    async fn follow(&self, over: &ShardConfig, held: &ShardConfig) -> Proposed {
        let shard = self.shard.shard();
        if !held.is_member(&self.replication.inner.node) {
            // Removed: it grants nothing more.
            tracing::info!(%shard, epoch = %held.epoch, "removed while proposing a takeover");
            return Proposed::Done;
        }
        let adopted = self.shard.reconfigure(held).await;
        // The old primary removed other members before it went silent: the
        // member's grace has still passed, so it proposes again over that.
        if held.primary == over.primary && adopted.is_ok() {
            return Proposed::Again;
        }
        tracing::info!(%shard, epoch = %held.epoch, primary = %held.primary, "a takeover lost");
        self.grace.resume();
        Proposed::Lost
    }
}
