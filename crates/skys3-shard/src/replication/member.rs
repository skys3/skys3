//! A member's end of a link from its shard's primary.

use std::sync::{Mutex, PoisonError};

use bytes::Bytes;
use skys3_io::Disk;
use skys3_log::{LogRecord, ShardRef};
use skys3_net::{Frame, MessageKind, Network, PeerIdentity, Receiver, Sender};
use skys3_types::{Epoch, Seq};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;

use super::wire::{self, Append, AppendAck, Beacon, Sync, SyncAck};
use super::{Inner, LinkError, ReplicationConfig, recv};
use crate::lease::Grace;
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

/// Serves one link from a primary, whose first frame is `frame`: a session
/// for one shard, until the link fails or a later session replaces it.
pub(super) async fn serve<S, N, D>(
    (mut receiver, mut sender): (Receiver<S>, Sender<S>),
    frame: Frame,
    inner: &Inner<N, D>,
) -> Result<(), LinkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    N: Network,
    D: Disk,
{
    let config = inner.config;
    let request: Sync = wire::body(&frame, MessageKind::Sync).map_err(LinkError::Protocol)?;
    let (shard, epoch) = match accept(&request, receiver.peer(), &inner.set).await {
        Ok(accepted) => accepted,
        Err((epoch, refused)) => {
            let answer = SyncAck {
                epoch: epoch.get(),
                refused: refused.clone(),
                ..SyncAck::default()
            };
            sender
                .send(&wire::frame(MessageKind::SyncAck, &answer, Bytes::new()))
                .await?;
            return Err(LinkError::Protocol(refused));
        }
    };
    let session = shard.begin_session();
    shard.settle().await;
    let last = *shard.durable().borrow();
    let answer = |done| SyncAck {
        last: last.get(),
        done,
        epoch: epoch.get(),
        refused: String::new(),
    };
    let primary_last = Seq::new(request.primary_last);
    if last > primary_last {
        for (_, record) in shard.read_tail(primary_last, last).await? {
            let frame = wire::frame(MessageKind::SyncAck, &answer(false), record);
            sender.send(&frame).await?;
        }
    }
    sender
        .send(&wire::frame(
            MessageKind::SyncAck,
            &answer(true),
            Bytes::new(),
        ))
        .await?;
    let beacons = Notify::new();
    // A replica opened outside `Replication::open` starts its grace with
    // its first session.
    let grace = inner.grace_of(shard.shard());
    let follow = Follower {
        shard: &shard,
        session,
        epoch,
        config,
        beacons: &beacons,
        grace: &grace,
        stamp: Mutex::new(None),
    };
    let refused = tokio::select! {
        biased;
        followed = follow.take(&mut receiver) => followed,
        acked = follow.acknowledge(&mut sender) => acked,
    };
    if let Err(Refusal::OlderEpoch) = refused {
        let ack = AppendAck {
            durable: shard.durable().borrow().get(),
            rejected: epoch.get(),
            lease: None,
        };
        sender
            .send(&wire::frame(MessageKind::AppendAck, &ack, Bytes::new()))
            .await?;
        return Err(LinkError::Protocol("an append of an older epoch".into()));
    }
    refused.map_err(|refusal| match refusal {
        Refusal::Link(error) => error,
        Refusal::OlderEpoch => unreachable!("answered above"),
    })
}

/// Finds the shard a `Sync` names, which must be open on this node as a
/// member, and checks that the peer is its primary in its epoch. A refusal
/// carries the member's epoch, if it knows the shard, and why.
async fn accept<D: Disk>(
    request: &Sync,
    peer: &PeerIdentity,
    set: &ShardSet<D>,
) -> Result<(Shard<D>, Epoch), (Epoch, String)> {
    let refuse = |epoch, reason: &str| Err((epoch, reason.to_owned()));
    let theirs = request.configuration().map_err(|e| (Epoch::ZERO, e))?;
    let shard = ShardRef::new(theirs.bucket_id.clone(), theirs.shard);
    let Some(replica) = set.get(&shard).await else {
        return refuse(Epoch::ZERO, "the shard is not open on this node");
    };
    let ours = replica.config();
    if replica.role() != Role::Member {
        return refuse(
            ours.epoch,
            "this node is not a member that follows a primary",
        );
    }
    if !matches!(peer, PeerIdentity::Node(node) if *node == theirs.primary) {
        return refuse(ours.epoch, "the peer is not the primary it names");
    }
    if theirs.epoch < ours.epoch {
        return refuse(ours.epoch, "the primary's epoch is older than the member's");
    }
    if theirs != ours {
        return refuse(
            ours.epoch,
            "the primary's configuration differs from the member's",
        );
    }
    Ok((replica, ours.epoch))
}

/// Why following a primary stopped.
enum Refusal {
    /// An append came from an older epoch (rule R2).
    OlderEpoch,
    /// The link failed, or the primary broke the protocol.
    Link(LinkError),
}

impl<E: Into<LinkError>> From<E> for Refusal {
    fn from(error: E) -> Self {
        Self::Link(error.into())
    }
}

/// A member following its primary in one session.
struct Follower<'a, D: Disk> {
    shard: &'a Shard<D>,
    session: u64,
    epoch: Epoch,
    config: ReplicationConfig,
    beacons: &'a Notify,
    /// The shard's grace, which every acknowledgement that grants a lease
    /// restarts.
    grace: &'a Grace,
    /// The latest lease stamp received in this session.
    stamp: Mutex<Option<u64>>,
}

impl<D: Disk> Follower<'_, D> {
    /// Takes the primary's appends and beacons.
    async fn take<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        receiver: &mut Receiver<S>,
    ) -> Result<(), Refusal> {
        loop {
            let frame = recv(receiver, self.config.link_timeout).await?;
            let commit = match frame.header.kind {
                MessageKind::Append => {
                    let append: Append =
                        wire::body(&frame, MessageKind::Append).map_err(LinkError::Protocol)?;
                    self.check_epoch(append.epoch)?;
                    self.stamped(append.lease);
                    let (record, _) = LogRecord::decode(&frame.payload)
                        .map_err(|error| LinkError::Protocol(error.to_string()))?;
                    let taken = self.shard.receive(
                        Some(self.session),
                        self.epoch,
                        record,
                        frame.payload,
                        append.lazy,
                    )?;
                    if !taken {
                        return Err(LinkError::Protocol("an append repeats a record".into()).into());
                    }
                    append.commit
                }
                _ => {
                    let beacon: Beacon =
                        wire::body(&frame, MessageKind::Beacon).map_err(LinkError::Protocol)?;
                    self.check_epoch(beacon.epoch)?;
                    self.stamped(beacon.lease);
                    self.beacons.notify_one();
                    beacon.commit
                }
            };
            self.shard.commit_through(Seq::new(commit));
        }
    }

    /// Keeps the lease stamp of a frame from the primary, if it has one.
    fn stamped(&self, lease: Option<u64>) {
        if lease.is_some() {
            *self.stamp.lock().unwrap_or_else(PoisonError::into_inner) = lease;
        }
    }

    fn check_epoch(&self, epoch: u64) -> Result<(), Refusal> {
        match epoch.cmp(&self.epoch.get()) {
            std::cmp::Ordering::Less => Err(Refusal::OlderEpoch),
            std::cmp::Ordering::Equal => Ok(()),
            std::cmp::Ordering::Greater => Err(LinkError::Protocol(format!(
                "an append of epoch {epoch}, newer than the member's {}",
                self.epoch
            ))
            .into()),
        }
    }

    /// Acknowledges the run of records held durably as it grows, and
    /// answers every beacon. Each acknowledgement echoes the latest lease
    /// stamp, which grants the primary a lease, so it restarts the grace
    /// before it leaves (§5.4).
    async fn acknowledge<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        sender: &mut Sender<S>,
    ) -> Result<(), Refusal> {
        let mut durable = self.shard.durable();
        loop {
            tokio::select! {
                biased;
                changed = durable.changed() => {
                    if changed.is_err() {
                        return Err(LinkError::Closed.into());
                    }
                }
                () = self.beacons.notified() => {}
            }
            let lease = *self.stamp.lock().unwrap_or_else(PoisonError::into_inner);
            if lease.is_some() {
                self.grace.grant();
            }
            let ack = AppendAck {
                durable: durable.borrow_and_update().get(),
                rejected: 0,
                lease,
            };
            sender
                .send(&wire::frame(MessageKind::AppendAck, &ack, Bytes::new()))
                .await?;
        }
    }
}
