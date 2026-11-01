//! A primary's link to one member for one shard.

use std::sync::Arc;

use bytes::Bytes;
use skys3_io::{Disk, MonoTime};
use skys3_log::LogRecord;
use skys3_net::{MessageKind, Network, Receiver, Sender, Transport};
use skys3_types::{Epoch, NodeAddress, NodeId, RegisterDocument, Seq, ShardConfig};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

use super::wire::{self, Append, AppendAck, Beacon, StepDown, Sync, SyncAck};
use super::{LinkError, ReplicationConfig, recv};
use crate::leader::{Leader, Pending};
use crate::shard::Shard;

/// How many records a link takes from the leader at a time.
const BATCH: usize = 64;

pub(super) struct Link<N: Network, D: Disk> {
    pub shard: Shard<D>,
    pub leader: Arc<Leader>,
    pub member: NodeId,
    pub address: NodeAddress,
    pub transport: Transport<N>,
    pub config: ReplicationConfig,
    /// Whether a watchdog checks the shard's register when a member
    /// refuses an append for a newer epoch.
    pub watched: bool,
}

/// What a session told the member: the primary's configuration, and the
/// epoch it sequenced in. A change of either opens a new session.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    config: ShardConfig,
    sequencing: Epoch,
}

impl<N: Network, D: Disk> Link<N, D> {
    /// Keeps the link up until the shard stops, its configuration leaves
    /// the member or learner out (§6.4), or the primary steps down for a
    /// planned handoff and owes the member nothing (§5.4).
    pub(super) async fn run(self) {
        while !self.shard.is_stopped()
            && self.names_member()
            && !self.leader.handed_off(&self.member)
        {
            match self.connect().await {
                // A new session, at once.
                Err(LinkError::Reconfigured) => continue,
                Err(error) => tracing::debug!(
                    shard = %self.leader.shard(),
                    member = %self.member,
                    %error,
                    "a replication link dropped",
                ),
                Ok(()) => {}
            }
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    /// Connects, opens a session, and replicates until the link fails or
    /// the primary changes its configuration.
    async fn connect(&self) -> Result<(), LinkError> {
        let connection = self.transport.connect(&self.member, &self.address).await?;
        let (mut receiver, mut sender) = connection.into_split();
        let (session, last, keeps) = self.sync(&mut receiver, &mut sender).await?;
        // A learner keeps its log, or installs a snapshot and ends the
        // session (§6.7).
        if self.leader.is_learner(&self.member) {
            let link = (&mut receiver, &mut sender);
            if !self.catch_up(link, &session.config, last, keeps).await? {
                return Ok(());
            }
        }
        self.leader.synced(&self.member, last);
        // The CONFIG record of a new configuration may have waited for
        // this member's report (§5.1).
        self.shard.align();
        tokio::select! {
            biased;
            sent = self.send(&mut sender, last, &session) => sent,
            heard = self.hear(&mut receiver) => heard,
        }
    }

    /// Whether the primary's configuration names the member, as a member
    /// or a learner, and the primary still sends it its log.
    fn names_member(&self) -> bool {
        let config = self.shard.config();
        (config.is_member(&self.member) || config.is_learner(&self.member))
            && self.leader.links_to(&self.member)
    }

    /// The session the primary opens now.
    fn session(&self) -> Session {
        Session {
            config: self.shard.config(),
            sequencing: self.shard.sequencing(),
        }
    }

    /// Opens a session: rolls forward the records the member holds past
    /// the primary's log, and returns the session, the member's last
    /// `seq`, and what a learner may keep of its log (§6.7): nothing
    /// (`None`), what reconciliation kept (the zero epoch), or its log if
    /// the primary holds a record of this epoch at its last `seq`.
    async fn sync<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        receiver: &mut Receiver<S>,
        sender: &mut Sender<S>,
    ) -> Result<(Session, Seq, Option<Epoch>), LinkError> {
        let session = self.session();
        let config = &session.config;
        let lineage = self.shard.lineage();
        let request = Sync {
            config: config
                .to_json()
                .map_err(|error| LinkError::Protocol(error.to_string()))?,
            primary_last: lineage.last().seq.get(),
            sequencing: session.sequencing.get(),
            ..Sync::default()
        }
        .with_lineage(&lineage);
        sender
            .send(&wire::frame(MessageKind::Sync, &request, Bytes::new()))
            .await?;
        loop {
            let frame = recv(receiver, self.config.link_timeout).await?;
            let answer: SyncAck =
                wire::body(&frame, MessageKind::SyncAck).map_err(LinkError::Protocol)?;
            if !answer.refused.is_empty() {
                self.newer_epoch(Epoch::new(answer.epoch));
                return Err(LinkError::Protocol(format!(
                    "the member refused the session in its epoch {}: {}",
                    answer.epoch, answer.refused
                )));
            }
            if !frame.payload.is_empty() {
                let (record, _) = LogRecord::decode(&frame.payload)
                    .map_err(|error| LinkError::Protocol(error.to_string()))?;
                self.shard
                    .receive(None, config.epoch, record, frame.payload, false)?;
            }
            if answer.done {
                // What a learner keeps: nothing, or what the primary
                // verifies, or what reconciliation kept (§6.7).
                let keeps = match (answer.fresh, answer.unverified) {
                    (true, _) => None,
                    (false, 0) => Some(Epoch::ZERO),
                    (false, epoch) => Some(Epoch::new(epoch)),
                };
                return Ok((session, Seq::new(answer.last), keeps));
            }
        }
    }

    /// Sends the member every record after `cursor` as the primary holds
    /// it, and beacons with the commit watermark in between, at least as
    /// often as [`ReplicationConfig::beacon_every`] says, so the member
    /// renews the primary's lease, and the link stays up, even while its
    /// log is slow to sync (§5.4). Returns [`LinkError::Reconfigured`] once
    /// the primary's configuration, or the epoch it sequences in, is no
    /// longer the session's: the member needs a new session to follow
    /// (§5.1).
    async fn send<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        sender: &mut Sender<S>,
        mut cursor: Seq,
        session: &Session,
    ) -> Result<(), LinkError> {
        let epoch = session.config.epoch.get();
        let mut changes = self
            .leader
            .subscribe(&self.member)
            .ok_or_else(|| LinkError::Protocol(format!("{} is not a member", self.member)))?;
        let mut last_beacon: Option<Instant> = None;
        let mut durable = self.shard.durable();
        loop {
            changes.borrow_and_update();
            if self.session() != *session {
                return Err(LinkError::Reconfigured);
            }
            let mut records = match self.leader.records_after(cursor, BATCH) {
                Pending::Ready(records) => records,
                Pending::InLog(through) => {
                    let records = match self.shard.read_tail(cursor, through).await {
                        Ok(records) => records,
                        Err(error) => {
                            // A learner behind what the log still holds
                            // gets a snapshot next (§6.7).
                            if self.leader.is_learner(&self.member) {
                                self.leader.needs_snapshot(&self.member);
                            }
                            return Err(error.into());
                        }
                    };
                    self.leader.restore(cursor, records);
                    continue;
                }
            };
            // A learner gets only what the primary holds durably: a primary
            // that restarts waits for its members' logs, not its learners',
            // before it gives a `seq` it lost a second record (§5.1, §6.4).
            let learner = self.leader.is_learner(&self.member);
            if learner {
                let held = *durable.borrow_and_update();
                records.retain(|record| record.seq <= held);
            }
            if records.is_empty() && self.shard.is_stopped() {
                return Ok(());
            }
            // A planned handoff: the member holds every record now. The
            // candidate gets the step-down message, and every link ends,
            // so that no beacon renews a member's grace any more (§5.4).
            if records.is_empty() && self.leader.handed_off(&self.member) {
                return Ok(());
            }
            if records.is_empty()
                && let Some((epoch, last)) = self.leader.step_down_owed(&self.member)
            {
                let step_down = StepDown {
                    epoch: epoch.get(),
                    last: last.get(),
                };
                sender
                    .send(&wire::frame(
                        MessageKind::StepDown,
                        &step_down,
                        Bytes::new(),
                    ))
                    .await?;
                self.leader.step_down_sent();
                // The member ends the session once it stops acknowledging
                // to propose itself; until then the link hears it out.
                return std::future::pending().await;
            }
            let interval = self.config.beacon_every(records.is_empty());
            if last_beacon.is_none_or(|at| Instant::now() >= at + interval) {
                let beacon = Beacon {
                    epoch,
                    commit: self.leader.commit().get(),
                    lease: self.stamp(),
                };
                sender
                    .send(&wire::frame(MessageKind::Beacon, &beacon, Bytes::new()))
                    .await?;
                last_beacon = Some(Instant::now());
            }
            if records.is_empty() {
                // Woken by a new record, a learner by one becoming durable
                // here, or for the next beacon. The watermark waits for
                // either.
                let every = self.config.beacon_every(true);
                let due = last_beacon.map_or_else(Instant::now, |at| at + every);
                let woken = async {
                    // Polled in order, not at random, so a simulation
                    // seed replays exactly.
                    tokio::select! {
                        biased;
                        _ = changes.changed() => {}
                        _ = durable.changed(), if learner => {}
                    }
                };
                let _ = tokio::time::timeout_at(due, woken).await;
                continue;
            }
            for record in records {
                if record.epoch > session.sequencing {
                    return Err(LinkError::Reconfigured);
                }
                let append = Append {
                    epoch,
                    commit: self.leader.commit().get(),
                    lazy: record.lazy,
                    lease: self.stamp(),
                };
                sender
                    .send(&wire::frame(MessageKind::Append, &append, record.bytes))
                    .await?;
                cursor = record.seq;
            }
        }
    }

    /// Acts on a member that refused a session or an append in its
    /// `epoch`: if that is newer than the primary's, this primary may be
    /// deposed (§6.5). Its watchdog checks the register; without one,
    /// nothing but a takeover moves the epoch past it, so it stops.
    fn newer_epoch(&self, epoch: Epoch) {
        if epoch <= self.shard.config().epoch {
            return;
        }
        if self.watched {
            self.leader.rejected(epoch);
        } else {
            self.shard
                .depose("a member refused the primary for a newer epoch", None);
        }
    }

    /// The lease stamp for a frame sent now.
    fn stamp(&self) -> Option<u64> {
        self.leader.lease_stamp().map(MonoTime::as_nanos)
    }

    /// Takes the member's acknowledgements, and the leases they grant,
    /// until the link fails.
    async fn hear<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        receiver: &mut Receiver<S>,
    ) -> Result<(), LinkError> {
        loop {
            let frame = recv(receiver, self.config.link_timeout).await?;
            let ack: AppendAck =
                wire::body(&frame, MessageKind::AppendAck).map_err(LinkError::Protocol)?;
            if ack.rejected != 0 {
                self.newer_epoch(Epoch::new(ack.rejected));
                return Err(LinkError::Protocol(format!(
                    "the member refused an append: it is in epoch {}",
                    ack.rejected
                )));
            }
            if let Some(stamp) = ack.lease {
                self.leader
                    .granted(&self.member, MonoTime::from_nanos(stamp));
            }
            self.leader
                .acknowledged(&self.member, Seq::new(ack.durable));
        }
    }
}
