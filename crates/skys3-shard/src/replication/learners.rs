//! Learners (§6.4, §6.7): a primary streams its log to the learners of its
//! configuration, takes each into the acknowledgement set once it keeps
//! up, drops it again without a compare-and-swap when it stops responding,
//! and promotes it to member by a compare-and-swap of the shard's register
//! once its backfill is complete and it is durable up to the commit
//! watermark (rule R3, §6.3).
//!
//! - **Joining.** A learner joins the acknowledgement set once it has
//!   reported its log in the primary's life, responds, and holds every
//!   record the primary had sequenced a check ago: it then stores new
//!   records as they come, so new writes have its copy too. A primary
//!   sends a learner only records it holds durably itself
//!   ([`Leader`](crate::Leader)), so each write waits for the primary's
//!   sync and then the learner's while the learner is in the set.
//! - **Dropping.** A learner in the set that does not respond for
//!   `member_suspect_after`, as a member would be removed for, leaves the
//!   set; the configuration does not change, since the commit rule does
//!   not count learners. It must catch up again before it rejoins.
//! - **Backfill.** The primary starts a learner's [`Backfill`] once the
//!   learner has reported its log. Without one, the live stream is the
//!   backfill: it brings the learner the primary's log from the record
//!   after the learner's last, from the first record for a new learner,
//!   and this build reclaims no log segment (plan M1-22), so a learner
//!   holds every record up to what it acknowledges.
//! - **Promoting.** The primary proposes the next configuration, with the
//!   learner a member, once the learner is in the set, its backfill is
//!   complete, and it is durable up to everything the primary may commit.
//!   It records the proposal durably in its index first, and until it
//!   learns the outcome, across restarts too, commits keep waiting for the
//!   learner and reads need its lease: a proposal that landed unseen has
//!   made the learner a member.
//!   Commits never wait for the compare-and-swap itself. A proposal that
//!   loses follows the register: the primary adopts the configuration
//!   there if it can, which settles the promotion, and proposes again on a
//!   later check if the learner is still a learner; otherwise it is
//!   deposed. A proposal whose answer is lost is sent again, unchanged,
//!   until the register settles it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_types::{NodeId, Seq, ShardConfig};
use tokio::time::Instant;

use super::lock;
use super::removal::{BoxFuture, Replaced, Seen, Watchdog};

/// Brings a learner the history of a shard that the live stream does not:
/// the extension point for backfill (§6.7, plan M2-15).
///
/// The primary calls [`Backfill::backfill`] once a learner of the shard has
/// reported its log in the primary's life, while the live stream runs, and
/// promotes the learner only after it returns `Ok`. A failure is retried on
/// a later check.
pub trait Backfill: Send + Sync + 'static {
    /// Brings `learner` the history of `shard` it lacks, and returns once
    /// it holds it durably, or why it could not.
    fn backfill<'a>(
        &'a self,
        shard: &'a ShardRef,
        learner: &'a NodeId,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// The learners whose backfill is running, by shard watchdog.
pub(super) type Backfilling = Arc<Mutex<BTreeSet<NodeId>>>;

impl<D: Disk> Watchdog<D> {
    /// Takes into the acknowledgement set the learners that keep up, drops
    /// those that stopped responding, and starts the backfill of those
    /// that reported their log: see the [module](self) docs. `seen` and
    /// `sequenced` are the members' measure of responding (§6.4).
    pub(super) fn tend_learners(
        &self,
        seen: &mut BTreeMap<NodeId, Seen>,
        now: Instant,
        sequenced: Seq,
        backfilling: &Backfilling,
    ) {
        let shard = self.shard.shard();
        let acking = self.leader.acking();
        for learner in self.leader.learners() {
            let Some(&learned) = self.observe(seen, &learner, now, sequenced) else {
                continue;
            };
            // It has not reported its log in this life.
            if learned.heard == 0 {
                continue;
            }
            self.start_backfill(&learner, backfilling);
            let silent = learned.silent(now) >= self.config.member_suspect_after;
            if acking.contains(&learner) {
                if silent && self.leader.drop_learner(&learner) {
                    tracing::info!(%shard, %learner, "dropped an unresponsive learner from the acknowledgement set");
                }
            } else if !silent && learned.acked >= sequenced && self.leader.join(&learner) {
                tracing::info!(%shard, %learner, "a learner joined the acknowledgement set");
            }
        }
    }

    /// Starts the backfill of `learner`, unless it is complete or running.
    fn start_backfill(&self, learner: &NodeId, backfilling: &Backfilling) {
        if self.leader.is_backfilled(learner) {
            return;
        }
        let Some(backfill) = self.backfill.clone() else {
            self.leader.backfilled(learner);
            return;
        };
        if !lock(backfilling).insert(learner.clone()) {
            return;
        }
        let (shard, leader) = (self.shard.shard().clone(), Arc::clone(&self.leader));
        let (learner, backfilling) = (learner.clone(), Arc::clone(backfilling));
        tokio::spawn(async move {
            match backfill.backfill(&shard, &learner).await {
                Ok(()) => {
                    tracing::info!(%shard, %learner, "a learner's backfill is complete");
                    leader.backfilled(&learner);
                }
                Err(error) => {
                    tracing::warn!(%shard, %learner, %error, "backfilling a learner failed; trying again");
                }
            }
            lock(&backfilling).remove(&learner);
        });
    }

    /// Settles the promotion outstanding, if there is one, or proposes the
    /// promotion of a learner that R3 allows: see the [module](self) docs.
    /// The caller holds the leader's lock for changing the register.
    pub(super) async fn promote(&self) {
        if let Some(proposed) = self.leader.promoting() {
            // Once the replica is in the proposal's epoch, the outcome is
            // known, and the leader forgets it with the CONFIG record.
            if self.shard.config().epoch < proposed.epoch {
                self.settle(&proposed).await;
            }
            return;
        }
        if !self.shard.is_serving() {
            return;
        }
        let Some(learner) = self
            .leader
            .acking()
            .into_iter()
            .find(|learner| self.leader.is_backfilled(learner))
        else {
            return;
        };
        let current = self.shard.config();
        let (shard, Some(epoch)) = (self.shard.shard(), current.epoch.checked_next()) else {
            return;
        };
        if !current.is_learner(&learner) {
            return;
        }
        let next = ShardConfig {
            epoch,
            members: current.members.iter().chain([&learner]).cloned().collect(),
            learners: current
                .learners
                .iter()
                .filter(|other| **other != learner)
                .cloned()
                .collect(),
            proposal_id: self.removal.next_id(),
            ..current.clone()
        };
        if let Err(why) = self.leader.begin_promotion(&learner, &next) {
            tracing::debug!(%shard, %learner, %why, "not promoting a learner yet");
            return;
        }
        // Durable before the compare-and-swap (§6.3).
        if let Err(error) = self.shard.record_promotion(&next).await {
            self.leader.abandon_promotion(&next);
            tracing::warn!(%shard, %learner, %error, "recording a promotion failed");
            return;
        }
        tracing::info!(%shard, %learner, %epoch, "promoting a learner");
        self.settle(&next).await;
    }

    /// Sends the outstanding promotion `proposed` to the shard's register,
    /// over the configuration it was built from, and acts on the outcome.
    /// A failed request leaves it outstanding, for a later check to send
    /// again. Once the replica has moved past that configuration, the
    /// outcome is known and nothing is sent: the replica's configuration
    /// is the register's, whose epoch the proposal has, or a later one.
    async fn settle(&self, proposed: &ShardConfig) {
        let shard = self.shard.shard();
        let current = self.shard.config();
        if current.epoch.checked_next() != Some(proposed.epoch)
            || self.leader.promoting().as_ref() != Some(proposed)
        {
            return;
        }
        match self.removal.registers.replace(&current, proposed).await {
            Ok(Replaced::Accepted) => {
                tracing::info!(%shard, epoch = %proposed.epoch, "promoted a learner");
                self.adopt(proposed);
            }
            Ok(Replaced::Holds(held)) => {
                tracing::info!(%shard, ?held, "a promotion lost");
                self.follow(&current, held.as_ref());
            }
            Err(error) => {
                tracing::warn!(%shard, %error, "promoting a learner failed; trying again");
            }
        }
    }
}
