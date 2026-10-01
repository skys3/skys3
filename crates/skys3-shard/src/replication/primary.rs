//! A primary's link to one member for one shard.

use std::sync::Arc;

use bytes::Bytes;
use skys3_io::{Disk, MonoTime};
use skys3_log::LogRecord;
use skys3_net::{MessageKind, Network, Receiver, Sender, Transport};
use skys3_types::{NodeAddress, NodeId, RegisterDocument, Seq};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

use super::wire::{self, Append, AppendAck, Beacon, Sync, SyncAck};
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
}

impl<N: Network, D: Disk> Link<N, D> {
    /// Keeps the link up until the shard stops.
    pub(super) async fn run(self) {
        while !self.shard.is_stopped() {
            if let Err(error) = self.connect().await {
                tracing::debug!(
                    shard = %self.leader.shard(),
                    member = %self.member,
                    %error,
                    "a replication link dropped",
                );
            }
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    /// Connects, opens a session, and replicates until the link fails.
    async fn connect(&self) -> Result<(), LinkError> {
        let connection = self.transport.connect(&self.member, &self.address).await?;
        let (mut receiver, mut sender) = connection.into_split();
        let last = self.sync(&mut receiver, &mut sender).await?;
        self.leader.synced(&self.member, last);
        tokio::select! {
            biased;
            sent = self.send(&mut sender, last) => sent,
            heard = self.hear(&mut receiver) => heard,
        }
    }

    /// Opens a session: rolls forward the records the member holds past
    /// the primary's log, and returns the member's last `seq`.
    async fn sync<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        receiver: &mut Receiver<S>,
        sender: &mut Sender<S>,
    ) -> Result<Seq, LinkError> {
        let config = self.shard.config();
        let request = Sync {
            config: config
                .to_json()
                .map_err(|error| LinkError::Protocol(error.to_string()))?,
            primary_last: self.shard.last_sequenced().get(),
        };
        sender
            .send(&wire::frame(MessageKind::Sync, &request, Bytes::new()))
            .await?;
        loop {
            let frame = recv(receiver, self.config.link_timeout).await?;
            let answer: SyncAck =
                wire::body(&frame, MessageKind::SyncAck).map_err(LinkError::Protocol)?;
            if !answer.refused.is_empty() {
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
                return Ok(Seq::new(answer.last));
            }
        }
    }

    /// Sends the member every record after `cursor` as the primary holds
    /// it, and beacons with the commit watermark in between: every
    /// `beacon_interval` while there is nothing to send, and at least every
    /// `lease_renew_interval` while there is, so the member renews the
    /// primary's lease even while its log is slow to sync (§5.4).
    async fn send<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        sender: &mut Sender<S>,
        mut cursor: Seq,
    ) -> Result<(), LinkError> {
        let epoch = self.shard.config().epoch.get();
        let mut changes = self
            .leader
            .subscribe(&self.member)
            .ok_or_else(|| LinkError::Protocol(format!("{} is not a member", self.member)))?;
        let mut last_beacon: Option<Instant> = None;
        loop {
            changes.borrow_and_update();
            let records = match self.leader.records_after(cursor, BATCH) {
                Pending::Ready(records) => records,
                Pending::InLog(through) => {
                    let records = self.shard.read_tail(cursor, through).await?;
                    self.leader.restore(cursor, records);
                    continue;
                }
            };
            if records.is_empty() && self.shard.is_stopped() {
                return Ok(());
            }
            let interval = if records.is_empty() {
                self.config.beacon_interval
            } else {
                self.config.lease_renew_interval
            };
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
                // Woken by a new record, or for the next beacon. The
                // watermark waits for either.
                let due =
                    last_beacon.map_or_else(Instant::now, |at| at + self.config.beacon_interval);
                let _ = tokio::time::timeout_at(due, changes.changed()).await;
                continue;
            }
            for record in records {
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
                // The member knows a newer epoch: this primary may be
                // deposed (plan M2-12 acts on it).
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
