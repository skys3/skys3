//! A primary's replication state: what each member holds, the records it
//! still has to send, and the commit rule (§5.1).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_io::{Clock, MonoTime};
use skys3_log::ShardRef;
use skys3_types::{Epoch, EpochSeq, NodeId, Seq, ShardConfig};
use tokio::sync::{mpsc, watch};

use crate::lease::Leases;
use crate::shard::{Message, Sequencer};

/// A record the primary sends its members: its position and its encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    /// The record's `seq`.
    pub seq: Seq,
    /// The record's epoch: that of the primary that sequenced it, which
    /// the tail of an earlier epoch keeps after a reconfiguration (§5.1).
    pub epoch: Epoch,
    /// The encoded record.
    pub bytes: Bytes,
    /// Whether the record may wait for another to start a group commit
    /// (see [`Shard::commit_lazy`](crate::Shard::commit_lazy)).
    pub lazy: bool,
}

/// What [`Leader::records_after`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pending {
    /// The records after the given `seq` that are ready to send, in order;
    /// empty when the member has every record sequenced so far.
    Ready(Vec<Outgoing>),
    /// The primary no longer holds the records after the given `seq` in
    /// memory: read them from the log up to this `seq`
    /// ([`Shard::read_tail`](crate::Shard::read_tail)) and hand them to
    /// [`Leader::restore`].
    InLog(Seq),
}

/// What the primary knows of one member in this life.
#[derive(Debug, Clone, Copy, Default)]
struct Progress {
    /// Whether the member reported its log since the primary started.
    synced: bool,
    /// The last `seq` the member holds durably, as far as the primary
    /// knows: acknowledgements are durable statements, so it only grows.
    acked: Seq,
    /// How many answers the member sent in this life: reports of its log,
    /// and acknowledgements, the answers to beacons included.
    heard: u64,
}

/// A promotion the primary proposed and does not know the outcome of
/// (§6.7).
#[derive(Debug, Clone)]
struct Promotion {
    /// The learner it makes a member.
    learner: NodeId,
    /// The configuration proposed.
    config: ShardConfig,
}

#[derive(Debug)]
struct State {
    /// The members other than the primary that commits and leases need, in
    /// configuration order.
    members: Vec<NodeId>,
    /// The learners the primary sends its log to, in configuration order.
    learners: Vec<NodeId>,
    /// The learners in the acknowledgement set: every commit waits for
    /// them too (§6.4).
    acking: BTreeSet<NodeId>,
    /// The learners whose backfill is complete (§6.7).
    backfilled: BTreeSet<NodeId>,
    /// The position of the snapshot last sent to each learner in this
    /// life: its backfill counts only from there on.
    snapshots: BTreeMap<NodeId, EpochSeq>,
    /// The learners whose next session gets a snapshot: the primary's log
    /// no longer holds the records after theirs.
    stale: BTreeSet<NodeId>,
    /// The promotion outstanding, if any: commits wait for its learner,
    /// and reads need its lease, until the primary knows the outcome.
    promoting: Option<Promotion>,
    /// What the primary knows of each member and learner.
    progress: BTreeMap<NodeId, Progress>,
    /// Every record after `base` the primary has sequenced or rolled
    /// forward: those not yet committed, which a member may still need,
    /// and those restored from the log for a member that is behind.
    buffer: BTreeMap<Seq, Outgoing>,
    /// The buffer holds every record after `base`; the log holds every
    /// record up to it.
    base: Seq,
    /// The commit limit the pipeline has: the lowest `seq` every member
    /// acknowledged.
    limit: Seq,
    /// The commit watermark: every record up to it is durable on every
    /// member, the primary included.
    commit: Seq,
    /// The last `seq` the primary must commit before it serves: its whole
    /// log once every member has reported.
    target: Option<Seq>,
}

/// A primary's replication state, shared by the shard and the links to its
/// members.
///
/// - **Sending.** Every record the primary sequences, or rolls forward from
///   a member, is kept until it commits, so a link can send it, and send it
///   again after reconnecting ([`Leader::records_after`]).
/// - **The commit rule.** A record commits once every member of the
///   configuration and the primary hold it durably (§5.1). Members
///   acknowledge the last `seq` of the run of records they hold durably
///   ([`Leader::acked`]); the shard's pipeline applies a record, and answers
///   its writer, only once it is durable here and every member's
///   acknowledgement covers it. [`Leader::commit`] is the watermark the
///   links send to members, which apply records up to it.
/// - **Reconciling.** A primary that opens, after a restart for example,
///   serves once every member has reported its log ([`Leader::synced`]),
///   every record any of them holds beyond the primary's own has been
///   rolled forward, and all of it is committed. Members hold no record the
///   primary did not sequence, so nothing needs truncating in its epoch.
/// - **Leases.** The primary serves reads only while it holds a lease from
///   every member (§5.4). Its links stamp what they send with
///   [`Leader::lease_stamp`], and each acknowledgement that echoes a stamp
///   grants a lease until the stamp plus `primary_lease`
///   ([`Leader::granted`]). Writes need no lease: the commit rule and
///   epochs fence them.
/// - **Removing members** (§6.4). Once the primary's `CONFIG` record of a
///   configuration without some members is durable, neither commits nor
///   leases need those members any more ([`Leader::members`]): the records
///   they held back commit, and their acknowledgements and grants count for
///   nothing from then on.
/// - **Learners** (§6.4, §6.7). The primary sends its log to the learners
///   of its configuration too ([`Leader::peers`]), but only the records it
///   holds durably, so a learner never holds a record that a restart of
///   the primary could give its `seq` to again. A learner the primary
///   takes into the acknowledgement set ([`Leader::join`]) holds up every
///   commit as a member does, until the primary drops it again
///   ([`Leader::drop_learner`]). Once its backfill is complete
///   ([`Leader::backfilled`]) and it is durable up to the commit
///   watermark, the primary proposes its promotion
///   ([`Leader::begin_promotion`]); until it knows the outcome, commits
///   wait for the learner and reads need its lease (§6.3).
pub struct Leader {
    shard: ShardRef,
    sequencer: Arc<Mutex<Sequencer>>,
    pipeline: mpsc::WeakUnboundedSender<Message>,
    durable: watch::Receiver<Seq>,
    state: Mutex<State>,
    /// Bumped when records are added, the watermark moves, or the members
    /// change: one channel per member and learner, in the order the leader
    /// learned of them, so that the links wake in the same order on every
    /// run (a channel with several receivers wakes them in an order chosen
    /// at random). A leaf lock.
    changes: Mutex<Vec<(NodeId, watch::Sender<u64>)>>,
    /// The members and learners the primary sends its log to.
    peers: watch::Sender<Vec<NodeId>>,
    links: AtomicBool,
    /// The leases from the members. A leaf lock: nothing else is locked
    /// while it is held.
    leases: Mutex<Leases>,
    /// The newest epoch a member refused an append for: another
    /// configuration may have replaced this one (§6.5).
    rejected: watch::Sender<Epoch>,
    /// Where a planned handoff stands (§5.4).
    handoff: watch::Sender<Handoff>,
    /// Held while the primary changes the shard's register, or hands the
    /// shard off: a handoff starts from the configuration the register
    /// holds, and no removal moves it on meanwhile.
    changing: tokio::sync::Mutex<()>,
}

/// Where a primary's planned handoff stands (§5.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Handoff {
    /// None was asked for.
    None,
    /// The primary stepped down, durably, and owes the candidate `to` a
    /// step-down message after its records up to `last`.
    Owed { to: NodeId, epoch: Epoch, last: Seq },
    /// The message was sent, or the primary gave up sending it.
    Over { sent: bool },
}

impl fmt::Debug for Leader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Leader")
            .field("shard", &self.shard)
            .field("members", &self.members())
            .finish_non_exhaustive()
    }
}

impl Leader {
    /// The leader of the primary of `config`. `promotion` is a promotion
    /// the primary recorded in an earlier life: unless `config` has reached
    /// its epoch, the outcome is unknown, so commits wait for its learner
    /// and reads need the learner's lease (§6.3).
    pub(crate) fn new(
        shard: ShardRef,
        config: &ShardConfig,
        promotion: Option<&ShardConfig>,
        sequencer: Arc<Mutex<Sequencer>>,
        pipeline: mpsc::UnboundedSender<Message>,
        durable: watch::Receiver<Seq>,
    ) -> Self {
        let members: Vec<NodeId> = config
            .members
            .iter()
            .filter(|member| **member != config.primary)
            .cloned()
            .collect();
        let mut learners = config.learners.clone();
        let promoting = promotion
            .filter(|proposed| proposed.epoch > config.epoch)
            .and_then(|proposed| {
                let learner = proposed.members.iter().find(|m| !config.is_member(m))?;
                if !learners.contains(learner) {
                    learners.push(learner.clone());
                }
                Some(Promotion {
                    learner: learner.clone(),
                    config: proposed.clone(),
                })
            });
        let peers: Vec<NodeId> = members.iter().chain(&learners).cloned().collect();
        let base = *durable.borrow();
        let state = State {
            members,
            learners,
            acking: BTreeSet::new(),
            backfilled: BTreeSet::new(),
            snapshots: BTreeMap::new(),
            stale: BTreeSet::new(),
            promoting,
            progress: peers
                .iter()
                .map(|peer| (peer.clone(), Progress::default()))
                .collect(),
            buffer: BTreeMap::new(),
            base,
            limit: Seq::ZERO,
            commit: Seq::ZERO,
            target: None,
        };
        let changes = peers
            .iter()
            .map(|peer| (peer.clone(), watch::Sender::new(0)))
            .collect();
        Self {
            shard,
            changes: Mutex::new(changes),
            peers: watch::Sender::new(peers),
            sequencer,
            pipeline: pipeline.downgrade(),
            durable,
            state: Mutex::new(state),
            links: AtomicBool::new(false),
            leases: Mutex::default(),
            rejected: watch::Sender::new(Epoch::ZERO),
            handoff: watch::Sender::new(Handoff::None),
            changing: tokio::sync::Mutex::new(()),
        }
    }

    /// The shard.
    #[must_use]
    pub fn shard(&self) -> &ShardRef {
        &self.shard
    }

    /// The members other than the primary that commits and leases need
    /// now, in configuration order.
    #[must_use]
    pub fn members(&self) -> Vec<NodeId> {
        self.state().members.clone()
    }

    /// Whether commits and leases need `member` now.
    #[must_use]
    pub fn is_member(&self, member: &NodeId) -> bool {
        self.state().members.contains(member)
    }

    /// The learners the primary sends its log to, in configuration order.
    #[must_use]
    pub fn learners(&self) -> Vec<NodeId> {
        self.state().learners.clone()
    }

    /// Whether `node` is one of [`Leader::learners`].
    #[must_use]
    pub fn is_learner(&self, node: &NodeId) -> bool {
        self.state().learners.contains(node)
    }

    /// The members and learners the primary sends its log to.
    #[must_use]
    pub fn peers(&self) -> watch::Receiver<Vec<NodeId>> {
        self.peers.subscribe()
    }

    /// Whether the primary sends its log to `node`.
    #[must_use]
    pub fn links_to(&self, node: &NodeId) -> bool {
        self.peers.borrow().contains(node)
    }

    /// The learners in the acknowledgement set, which every commit waits
    /// for (§6.4).
    #[must_use]
    pub fn acking(&self) -> Vec<NodeId> {
        self.state().acking.iter().cloned().collect()
    }

    /// Takes `learner` into the acknowledgement set: from now on every
    /// commit waits for it, and it counts as an acknowledging copy for
    /// `min_write_replicas` (§6.4). Call it once the learner holds the
    /// records the primary had sequenced a moment ago, so that it stores
    /// new records as they come. Returns whether it joined now.
    pub fn join(&self, learner: &NodeId) -> bool {
        let joined = {
            let mut state = self.state();
            let joined = state.learners.contains(learner) && state.acking.insert(learner.clone());
            lock(&self.sequencer).acking = state.acking.len();
            joined
        };
        self.progress();
        joined
    }

    /// Drops `learner` from the acknowledgement set, as when it missed
    /// `member_suspect_after` (§6.4): commits no longer wait for it, and it
    /// must catch up again before it rejoins. No configuration changes.
    /// The learner of an outstanding promotion stays: the primary keeps
    /// waiting for it until it knows the outcome. Returns whether it was
    /// dropped.
    pub fn drop_learner(&self, learner: &NodeId) -> bool {
        let dropped = {
            let mut state = self.state();
            if state
                .promoting
                .as_ref()
                .is_some_and(|p| p.learner == *learner)
            {
                return false;
            }
            let dropped = state.acking.remove(learner);
            lock(&self.sequencer).acking = state.acking.len();
            dropped
        };
        self.progress();
        dropped
    }

    /// Records that `learner`'s backfill is complete (§6.7): it holds the
    /// shard's history that the live stream did not bring it.
    pub fn backfilled(&self, learner: &NodeId) {
        let mut state = self.state();
        if state.learners.contains(learner) {
            state.backfilled.insert(learner.clone());
        }
    }

    /// Whether `learner`'s backfill is complete.
    #[must_use]
    pub fn is_backfilled(&self, learner: &NodeId) -> bool {
        self.state().backfilled.contains(learner)
    }

    /// Records that `learner` reported holding the payload of every entry
    /// it has, at its applied position `applied` (§6.7), and returns
    /// whether that completes its backfill: it does unless the primary
    /// sent it a snapshot since, which the report predates.
    pub(crate) fn fill_complete(&self, learner: &NodeId, applied: EpochSeq) -> bool {
        let mut state = self.state();
        let current = state
            .snapshots
            .get(learner)
            .is_none_or(|snapshot| applied >= *snapshot);
        let complete = current && state.learners.contains(learner);
        if complete {
            state.backfilled.insert(learner.clone());
        }
        complete
    }

    /// Records that the primary sends `learner` a snapshot taken at `at`
    /// in place of its log: its backfill starts over from there.
    pub(crate) fn snapshot_sent(&self, learner: &NodeId, at: EpochSeq) {
        let mut state = self.state();
        state.backfilled.remove(learner);
        state.stale.remove(learner);
        state.snapshots.insert(learner.clone(), at);
    }

    /// Records that the primary's log no longer holds the records after
    /// `learner`'s last: its next session gets a snapshot.
    pub(crate) fn needs_snapshot(&self, learner: &NodeId) {
        self.state().stale.insert(learner.clone());
    }

    /// Whether `learner`'s next session gets a snapshot because the
    /// primary's log no longer holds the records after its last.
    pub(crate) fn wants_snapshot(&self, learner: &NodeId) -> bool {
        self.state().stale.contains(learner)
    }

    /// The configuration of the promotion the primary proposed and does
    /// not know the outcome of, if any.
    #[must_use]
    pub fn promoting(&self) -> Option<ShardConfig> {
        self.state().promoting.as_ref().map(|p| p.config.clone())
    }

    /// Records that the primary proposes `config` to promote `learner`
    /// (§6.7), if R3 allows it now: the learner is in the acknowledgement
    /// set, its backfill is complete, it is durable up to every record the
    /// pipeline may commit, and no other promotion is outstanding. From
    /// then on, until the primary adopts a configuration at `config`'s
    /// epoch or later, commits wait for the learner and reads need its
    /// lease, since the proposal may land unseen. Commits do not pause:
    /// they only keep waiting for the learner, as they did.
    ///
    /// # Errors
    ///
    /// Why the learner cannot be promoted now.
    pub fn begin_promotion(&self, learner: &NodeId, config: &ShardConfig) -> Result<(), String> {
        let mut state = self.state();
        let acked = state.progress.get(learner).map_or(Seq::ZERO, |p| p.acked);
        let refusal = if state.promoting.is_some() {
            "another promotion is outstanding".to_owned()
        } else if !state.acking.contains(learner) {
            format!("{learner} is not in the acknowledgement set")
        } else if !state.backfilled.contains(learner) {
            format!("the backfill of {learner} is not complete")
        } else if acked < state.limit.max(state.commit) {
            format!("{learner} is durable only up to seq {acked}")
        } else if !config.is_member(learner) || config.is_learner(learner) {
            format!("the configuration does not make {learner} a member")
        } else {
            state.promoting = Some(Promotion {
                learner: learner.clone(),
                config: config.clone(),
            });
            return Ok(());
        };
        Err(refusal)
    }

    /// Forgets the outstanding promotion of `config`, which the primary
    /// did not propose after all: it could not record it.
    pub(crate) fn abandon_promotion(&self, config: &ShardConfig) {
        let mut state = self.state();
        if state
            .promoting
            .as_ref()
            .is_some_and(|p| p.config == *config)
        {
            state.promoting = None;
        }
    }

    /// Records that a member refused an append because it knows `epoch`,
    /// newer than the primary's (rule R2): the shard's register may name
    /// another configuration, which the primary must check (§6.5).
    pub(crate) fn rejected(&self, epoch: Epoch) {
        self.rejected
            .send_modify(|newest| *newest = (*newest).max(epoch));
    }

    /// A receiver that sees each refusal of an append for a newer epoch
    /// ([`Leader::rejected`]), with the newest such epoch.
    pub(crate) fn rejections(&self) -> watch::Receiver<Epoch> {
        self.rejected.subscribe()
    }

    /// The lock held while the primary changes the shard's register, by a
    /// removal (§6.4), or hands the shard off (§5.4).
    pub(crate) fn changing(&self) -> &tokio::sync::Mutex<()> {
        &self.changing
    }

    /// Stops renewing leases for good, as the first step of a planned
    /// handoff (§5.4): the primary holds none from then on.
    pub(crate) fn stop_leases(&self) {
        self.leases().stop();
    }

    /// Owes `to` the step-down message of a planned handoff in `epoch`,
    /// after the records up to `last` (§5.4). The links wake: the one to
    /// `to` sends it, and every other one ends.
    pub(crate) fn owe_step_down(&self, to: NodeId, epoch: Epoch, last: Seq) {
        self.handoff.send_replace(Handoff::Owed { to, epoch, last });
        self.changed();
    }

    /// The step-down owed to `member`, if one is: its epoch and last `seq`.
    pub(crate) fn step_down_owed(&self, member: &NodeId) -> Option<(Epoch, Seq)> {
        match &*self.handoff.borrow() {
            Handoff::Owed { to, epoch, last } if to == member => Some((*epoch, *last)),
            _ => None,
        }
    }

    /// Whether the link to `member` has nothing more to do because of a
    /// handoff: one is under way and owes `member` nothing.
    pub(crate) fn handed_off(&self, member: &NodeId) -> bool {
        match &*self.handoff.borrow() {
            Handoff::None => false,
            Handoff::Owed { to, .. } => to != member,
            Handoff::Over { .. } => true,
        }
    }

    /// Records that the owed step-down message was sent.
    pub(crate) fn step_down_sent(&self) {
        self.handoff.send_if_modified(|handoff| {
            let owed = matches!(handoff, Handoff::Owed { .. });
            if owed {
                *handoff = Handoff::Over { sent: true };
            }
            owed
        });
    }

    /// Waits up to `timeout` for the owed step-down message to be sent,
    /// then gives up on it: the candidate then waits out its grace
    /// instead. Returns whether it was sent.
    pub(crate) async fn await_step_down(&self, timeout: Duration) -> bool {
        let mut handoff = self.handoff.subscribe();
        let over = |h: &Handoff| matches!(h, Handoff::Over { .. });
        let _ = tokio::time::timeout(timeout, handoff.wait_for(over)).await;
        self.handoff.send_if_modified(|handoff| {
            let owed = matches!(handoff, Handoff::Owed { .. });
            if owed {
                *handoff = Handoff::Over { sent: false };
            }
            owed
        });
        self.changed();
        *self.handoff.borrow() == Handoff::Over { sent: true }
    }

    /// Returns `true` the first time it is called: whoever gets `true`
    /// starts the links to the members.
    pub fn claim_links(&self) -> bool {
        !self.links.swap(true, Ordering::SeqCst)
    }

    /// Times the members' leases on `clock`, each lasting `primary_lease`
    /// from the stamp it acknowledges. Until this is called the primary
    /// holds no lease, and serves no read.
    pub fn start_leases(&self, clock: Arc<dyn Clock>, primary_lease: Duration) {
        self.leases().start(clock, primary_lease);
    }

    /// The stamp for a beacon or append sent now: the primary's clock
    /// reading, or `None` before [`Leader::start_leases`].
    #[must_use]
    pub fn lease_stamp(&self) -> Option<MonoTime> {
        self.leases().stamp()
    }

    /// Records that `member` acknowledged the beacon or append stamped
    /// `stamp`, which grants a lease until `stamp + primary_lease`. Learners
    /// grant leases as members do (§5.4). A node the primary does not link
    /// to grants nothing, a member removed from the shard included.
    pub fn granted(&self, member: &NodeId, stamp: MonoTime) {
        if self.state().progress.contains_key(member) {
            self.leases().granted(member, stamp);
        }
    }

    /// When the lease from `member` ends on the primary's clock, if it ever
    /// granted one.
    #[must_use]
    pub fn lease(&self, member: &NodeId) -> Option<MonoTime> {
        self.leases().until(member)
    }

    /// Whether the primary holds a valid lease from every member now, and
    /// from the learner of an outstanding promotion, which may be a member
    /// already (§5.4, §6.7), as serving a read requires.
    #[must_use]
    pub fn holds_leases(&self) -> bool {
        let needed = {
            let state = self.state();
            let mut needed = state.members.clone();
            needed.extend(state.promoting.iter().map(|p| p.learner.clone()));
            needed
        };
        self.leases().held(&needed)
    }

    /// The commit watermark: every record up to it is durable on every
    /// member and on the primary.
    #[must_use]
    pub fn commit(&self) -> Seq {
        self.state().commit
    }

    /// The last `seq` `member` acknowledged as durable in this life.
    #[must_use]
    pub fn acked(&self, member: &NodeId) -> Option<Seq> {
        self.state().progress.get(member).map(|p| p.acked)
    }

    /// What `member` acknowledged in this life, and how many answers it
    /// sent: whether it still responds (§6.4).
    pub(crate) fn heard(&self, member: &NodeId) -> Option<(Seq, u64)> {
        self.state()
            .progress
            .get(member)
            .map(|p| (p.acked, p.heard))
    }

    /// Whether every member of `config` other than its primary has
    /// reported its log in this life, so that the primary's log holds every
    /// record any of them holds.
    #[must_use]
    pub fn synced_all(&self, config: &ShardConfig) -> bool {
        let state = self.state();
        config
            .members
            .iter()
            .filter(|member| **member != config.primary)
            .all(|member| state.progress.get(member).is_some_and(|p| p.synced))
    }

    /// A receiver for the link to `member` that sees a change whenever
    /// records are added, the commit watermark moves, or the members
    /// change, or `None` if the primary never linked to `member`.
    #[must_use]
    pub fn subscribe(&self, member: &NodeId) -> Option<watch::Receiver<u64>> {
        let changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
        let (_, changes) = changes.iter().find(|(node, _)| node == member)?;
        Some(changes.subscribe())
    }

    /// Wakes every link, in the order the leader learned of their peers.
    fn changed(&self) {
        let changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
        for (_, changes) in changes.iter() {
            changes.send_modify(|n| *n = n.wrapping_add(1));
        }
    }

    /// Keeps a record the primary sequenced or rolled forward, for its
    /// links to send.
    pub(crate) fn push(&self, position: EpochSeq, bytes: Bytes, lazy: bool) {
        let outgoing = Outgoing {
            seq: position.seq,
            epoch: position.epoch,
            bytes,
            lazy,
        };
        self.state().buffer.insert(position.seq, outgoing);
        self.changed();
    }

    /// Up to `max` records after `after` to send a member, or where to read
    /// them if the primary no longer holds them.
    #[must_use]
    pub fn records_after(&self, after: Seq, max: usize) -> Pending {
        let state = self.state();
        if after < state.base {
            return Pending::InLog(state.base);
        }
        let mut next = after.get().saturating_add(1);
        let mut ready = Vec::new();
        while ready.len() < max {
            let Some(record) = state.buffer.get(&Seq::new(next)) else {
                break;
            };
            ready.push(record.clone());
            next = next.saturating_add(1);
        }
        Pending::Ready(ready)
    }

    /// Keeps records read from the log after `after`, which the primary no
    /// longer held in memory (see [`Pending::InLog`]).
    pub fn restore(&self, after: Seq, records: Vec<(EpochSeq, Bytes)>) {
        let mut state = self.state();
        for (position, bytes) in records {
            state.buffer.entry(position.seq).or_insert(Outgoing {
                seq: position.seq,
                epoch: position.epoch,
                bytes,
                lazy: false,
            });
        }
        state.base = state.base.min(after);
    }

    /// Records that `member` reported holding every record up to `last`
    /// durably, after rolling forward any it held beyond the primary's log.
    /// Once every member has reported, the primary's log is complete, and
    /// it serves once all of it is committed.
    pub fn synced(&self, member: &NodeId, last: Seq) {
        {
            let mut state = self.state();
            let Some(progress) = state.progress.get_mut(member) else {
                return;
            };
            progress.synced = true;
            progress.acked = progress.acked.max(last);
            progress.heard += 1;
            self.target_if_synced(&mut state);
        }
        self.progress();
    }

    /// Sets the last `seq` the primary commits before it serves, its whole
    /// log, once every member has reported.
    fn target_if_synced(&self, state: &mut State) {
        let synced = |member: &NodeId| state.progress.get(member).is_some_and(|p| p.synced);
        if state.target.is_none() && state.members.iter().all(synced) {
            let next = lock(&self.sequencer).next.seq;
            state.target = Some(Seq::new(next.get().saturating_sub(1)));
        }
    }

    /// Records that `member` holds every record up to `seq` durably.
    pub fn acknowledged(&self, member: &NodeId, seq: Seq) {
        {
            let mut state = self.state();
            if let Some(progress) = state.progress.get_mut(member) {
                progress.acked = progress.acked.max(seq);
                progress.heard += 1;
            }
        }
        self.progress();
    }

    /// Adopts `config`, whose `CONFIG` record is now durable on the
    /// primary: commits and leases no longer need the members it leaves out
    /// (§6.4), so the records only they held back commit, and the links to
    /// them see the change and end. A learner it adds gets a link; a
    /// learner it promotes is a member from now on (§6.7); and a promotion
    /// outstanding is settled once `config` has reached its epoch, whether
    /// `config` is that promotion or a configuration it lost to.
    pub(crate) fn adopt(&self, config: &ShardConfig) {
        {
            let mut state = self.state();
            if state
                .promoting
                .as_ref()
                .is_some_and(|p| p.config.epoch <= config.epoch)
            {
                state.promoting = None;
            }
            state.members = config
                .members
                .iter()
                .filter(|member| **member != config.primary)
                .cloned()
                .collect();
            state.learners.clone_from(&config.learners);
            let State {
                members,
                learners,
                acking,
                backfilled,
                snapshots,
                stale,
                promoting,
                progress,
                ..
            } = &mut *state;
            // An outstanding promotion keeps its learner linked.
            if let Some(p) = promoting
                .as_ref()
                .filter(|p| !learners.contains(&p.learner))
            {
                learners.push(p.learner.clone());
            }
            let peers: Vec<NodeId> = members.iter().chain(learners.iter()).cloned().collect();
            progress.retain(|node, _| peers.contains(node));
            for peer in &peers {
                progress.entry(peer.clone()).or_default();
            }
            acking.retain(|node| learners.contains(node));
            backfilled.retain(|node| learners.contains(node));
            snapshots.retain(|node, _| learners.contains(node));
            stale.retain(|node| learners.contains(node));
            lock(&self.sequencer).acking = acking.len();
            {
                let mut changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
                for peer in &peers {
                    if !changes.iter().any(|(node, _)| node == peer) {
                        changes.push((peer.clone(), watch::Sender::new(0)));
                    }
                }
            }
            self.peers.send_if_modified(|linked| {
                let changed = *linked != peers;
                *linked = peers;
                changed
            });
            self.target_if_synced(&mut state);
        }
        self.changed();
        self.progress();
    }

    /// Applies the commit rule: raises the pipeline's commit limit to what
    /// every member, every learner in the acknowledgement set, and the
    /// learner of an outstanding promotion acknowledged, moves the
    /// watermark, drops committed records, and starts serving once
    /// reconciling is done.
    pub(crate) fn progress(&self) {
        let mut state = self.state();
        let needed = state
            .members
            .iter()
            .chain(&state.acking)
            .chain(state.promoting.iter().map(|p| &p.learner));
        let acked = needed
            .map(|node| state.progress.get(node).map_or(Seq::ZERO, |p| p.acked))
            .min()
            .unwrap_or(Seq::MAX);
        if acked > state.limit {
            state.limit = acked;
            if let Some(pipeline) = self.pipeline.upgrade() {
                let _ = pipeline.send(Message::Commit(acked));
            }
        }
        let commit = acked.min(*self.durable.borrow());
        if commit > state.commit {
            state.commit = commit;
            state.buffer = state
                .buffer
                .split_off(&Seq::new(commit.get().saturating_add(1)));
            state.base = state.base.max(commit);
            self.changed();
        }
        if let Some(target) = state.target {
            let mut sequencer = lock(&self.sequencer);
            if !sequencer.serving
                && sequencer.stepped_down.is_none()
                && sequencer.is_aligned()
                && commit >= target
                && sequencer.applied.seq >= target
            {
                sequencer.serving = true;
            }
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Every update leaves the state consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn leases(&self) -> MutexGuard<'_, Leases> {
        // Every update leaves the leases consistent.
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn lock(sequencer: &Mutex<Sequencer>) -> MutexGuard<'_, Sequencer> {
    sequencer.lock().unwrap_or_else(PoisonError::into_inner)
}
