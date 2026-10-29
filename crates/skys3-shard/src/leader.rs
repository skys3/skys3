//! A primary's replication state: what each member holds, the records it
//! still has to send, and the commit rule (§5.1).

use std::collections::BTreeMap;
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

#[derive(Debug)]
struct State {
    /// The members other than the primary that commits and leases need, in
    /// configuration order.
    members: Vec<NodeId>,
    /// What the primary knows of each of them.
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
pub struct Leader {
    shard: ShardRef,
    /// The members other than the primary when the leader started: the
    /// owners of the channels in `changes`.
    started: Vec<NodeId>,
    sequencer: Arc<Mutex<Sequencer>>,
    pipeline: mpsc::WeakUnboundedSender<Message>,
    durable: watch::Receiver<Seq>,
    state: Mutex<State>,
    /// Bumped when records are added, the watermark moves, or the members
    /// change: one channel per member, in the order of `started`, so that
    /// the links wake in the same order on every run (a channel with
    /// several receivers wakes them in an order chosen at random).
    changes: Vec<watch::Sender<u64>>,
    links: AtomicBool,
    /// The leases from the members. A leaf lock: nothing else is locked
    /// while it is held.
    leases: Mutex<Leases>,
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
    pub(crate) fn new(
        shard: ShardRef,
        config: &ShardConfig,
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
        let base = *durable.borrow();
        let state = State {
            members: members.clone(),
            progress: members
                .iter()
                .map(|member| (member.clone(), Progress::default()))
                .collect(),
            buffer: BTreeMap::new(),
            base,
            limit: Seq::ZERO,
            commit: Seq::ZERO,
            target: None,
        };
        Self {
            shard,
            changes: members.iter().map(|_| watch::Sender::new(0)).collect(),
            started: members,
            sequencer,
            pipeline: pipeline.downgrade(),
            durable,
            state: Mutex::new(state),
            links: AtomicBool::new(false),
            leases: Mutex::default(),
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
    /// `stamp`, which grants a lease until `stamp + primary_lease`. A node
    /// that is not one of [`Leader::members`] grants nothing, a member
    /// removed from the shard included.
    pub fn granted(&self, member: &NodeId, stamp: MonoTime) {
        if self.is_member(member) {
            self.leases().granted(member, stamp);
        }
    }

    /// When the lease from `member` ends on the primary's clock, if it ever
    /// granted one.
    #[must_use]
    pub fn lease(&self, member: &NodeId) -> Option<MonoTime> {
        self.leases().until(member)
    }

    /// Whether the primary holds a valid lease from every member now, as
    /// serving a read requires.
    #[must_use]
    pub fn holds_leases(&self) -> bool {
        let members = self.members();
        self.leases().held(&members)
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
    /// change, or `None` if `member` was never one of
    /// [`Leader::members`].
    #[must_use]
    pub fn subscribe(&self, member: &NodeId) -> Option<watch::Receiver<u64>> {
        let index = self.started.iter().position(|m| m == member)?;
        Some(self.changes[index].subscribe())
    }

    /// Wakes every link, in the order of the members.
    fn changed(&self) {
        for changes in &self.changes {
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
        if state.target.is_none() && state.progress.values().all(|p| p.synced) {
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
    /// them see the change and end. A configuration adds no member here.
    pub(crate) fn adopt(&self, config: &ShardConfig) {
        {
            let mut state = self.state();
            let State {
                members, progress, ..
            } = &mut *state;
            members.retain(|member| config.is_member(member) && *member != config.primary);
            progress.retain(|member, _| members.contains(member));
            self.target_if_synced(&mut state);
        }
        self.changed();
        self.progress();
    }

    /// Applies the commit rule: raises the pipeline's commit limit to what
    /// every member acknowledged, moves the watermark, drops committed
    /// records, and starts serving once reconciling is done.
    pub(crate) fn progress(&self) {
        let mut state = self.state();
        let acked = state
            .progress
            .values()
            .map(|p| p.acked)
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
