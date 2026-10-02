//! Backfill and re-admission (§6.4, §6.7): how a learner comes to hold the
//! history of its shard that the live stream does not bring it.
//!
//! - **Catching up.** Each session of a primary with a learner decides,
//!   right after the learner's `SyncAck`, whether the learner keeps its
//!   log and takes the primary's from the record after its last, or gets
//!   a snapshot. A learner keeps its log if lineage reconciliation kept it
//!   (§6.6), or, when the lineages cannot tell (a re-admitted node whose
//!   log, or the primary's, is known only from where it last opened), if
//!   the primary holds durably a record of the same epoch at the learner's
//!   last `seq`: one primary per epoch assigns each `seq` once, so the two
//!   logs then hold the same records up to there. Its old records then
//!   seed catch-up. A learner whose log is empty, unverified, or behind
//!   what the primary's log still holds gets a snapshot instead, which
//!   discards its records.
//! - **The snapshot** is the shard's index at the primary's applied
//!   position, which must be in the session's epoch: every record the
//!   live stream sends next is then of that epoch or a later one, and the
//!   learner's `TRUNCATE` at `(epoch, 0)` invalidates every record it held
//!   from before. The learner installs it and opens its replica again at
//!   that position, and the primary's next session streams the log from
//!   there. New writes have the learner's copy again as soon as it joins
//!   the acknowledgement set, within seconds of being added.
//! - **Payload.** The snapshot names payload by log position; the learner
//!   holds none of it. The primary's watchdog backfills it over a
//!   connection of its own, so the live stream never waits behind it: the
//!   learner asks for the positions its entries need and does not locate
//!   (those of every entry that is not clean, and of every open multipart
//!   upload: what has no other home), and stores the records the primary
//!   sends. Clean payload of `write_back` buckets is not copied: the
//!   snapshot carries those entries as evicted, since the remote holds
//!   them. Once a scan of its whole index finds nothing missing, the
//!   learner reports its applied position, and its backfill is complete
//!   if that is at or after the snapshot the primary last sent it
//!   ([`Leader::fill_complete`]). Promotion waits for that (rule R3).

use bytes::Bytes;
use skys3_index::ShardTable;
use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_net::{Frame, MessageKind, Network, PeerIdentity, Receiver, Sender};
use skys3_types::{Epoch, EpochSeq, NodeId, RegisterDocument, Seq, ShardConfig};
use tokio::io::{AsyncRead, AsyncWrite};

use super::primary::Link;
use super::wire::{self, Backfill, BackfillAck, Row, Run, SnapshotRows};
use super::{LinkError, Replication, recv};
use crate::leader::Leader;
use crate::shard::{FillCursor, Role, Shard};

/// How many positions a learner asks for at a time.
const NEEDS: usize = 64;

/// How long either end of a backfill waits for the other's next step,
/// which may include a scan of the index or a few syncs, in link timeouts.
const PATIENCE: u32 = 4;

fn protocol(error: impl ToString) -> LinkError {
    LinkError::Protocol(error.to_string())
}

impl<N: Network, D: Disk> Link<N, D> {
    /// Tells the learner of this link, which reported its last record at
    /// `last`, whether it keeps its log, or sends it a snapshot of the
    /// primary's configuration `config`: see the [module](self) docs.
    /// `keeps` is what the learner may keep of its log: nothing (`None`),
    /// the log reconciliation kept (the zero epoch), or, if lineage
    /// reconciliation could not tell its log from the primary's, its log
    /// if its record at `last` is of this epoch. Returns whether the
    /// session goes on to stream the log.
    pub(super) async fn catch_up<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        (receiver, sender): (&mut Receiver<S>, &mut Sender<S>),
        config: &ShardConfig,
        last: Seq,
        keeps: Option<Epoch>,
    ) -> Result<bool, LinkError> {
        let keep = match keeps {
            None => false,
            Some(Epoch::ZERO) => !self.leader.wants_snapshot(&self.member),
            Some(epoch) => self.shard.record_epoch(last).await == Some(epoch),
        };
        if keep {
            let verdict = Backfill {
                keep: true,
                ..Backfill::default()
            };
            let frame = wire::frame(MessageKind::Backfill, &verdict, Bytes::new());
            sender.send(&frame).await?;
            return Ok(true);
        }
        let (reader, at) = self.shard.snapshot().await?;
        if at.epoch < config.epoch {
            return Err(protocol(format_args!(
                "the primary has not applied a record of its epoch {} yet",
                config.epoch
            )));
        }
        let (shard, learner) = (self.leader.shard(), &self.member);
        tracing::info!(%shard, %learner, %at, "sending a learner a snapshot");
        self.leader.snapshot_sent(learner, at);
        let chunk = |done, rows: Vec<Row>| {
            let header = Backfill {
                epoch: at.epoch.get(),
                seq: at.seq.get(),
                done,
                ..Backfill::default()
            };
            let rows = SnapshotRows { rows }.to_payload();
            wire::frame(MessageKind::Backfill, &header, rows)
        };
        for table in ShardTable::ALL {
            let mut after = None;
            loop {
                let rows = self.shard.snapshot_rows(&reader, table, after).await?;
                after = rows.last().map(|(key, _)| key.clone());
                if after.is_none() {
                    break;
                }
                let rows = rows
                    .into_iter()
                    .map(|(key, value)| Row {
                        table: table.code(),
                        key,
                        value,
                    })
                    .collect();
                sender.send(&chunk(false, rows)).await?;
            }
        }
        sender.send(&chunk(true, Vec::new())).await?;
        let frame = recv(receiver, self.config.link_timeout * PATIENCE).await?;
        let ack: BackfillAck = wire::body(&frame, MessageKind::BackfillAck).map_err(protocol)?;
        if ack.refused.is_empty() {
            Ok(false)
        } else {
            Err(protocol(format_args!(
                "the learner refused the snapshot: {}",
                ack.refused
            )))
        }
    }
}

/// Installs the snapshot whose first chunk is `frame`, sent by the primary
/// of the learner `replica`'s session in place of its log: see the
/// [module](self) docs. The replica opens again, and the session ends;
/// the primary's next one streams the log from the snapshot on.
pub(super) async fn install<S, N, D>(
    replication: &Replication<N, D>,
    replica: &Shard<D>,
    mut frame: Frame,
    (receiver, sender): (&mut Receiver<S>, &mut Sender<S>),
) -> Result<(), LinkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    N: Network,
    D: Disk,
{
    let inner = &*replication.inner;
    let config = replica.config();
    let first: Backfill = wire::body(&frame, MessageKind::Backfill).map_err(protocol)?;
    let at = first.position();
    let shard = replica.shard().clone();
    let installed = if at.epoch < config.epoch {
        Err(format!(
            "the snapshot at {at} is older than the learner's epoch {}",
            config.epoch
        ))
    } else {
        tracing::info!(%shard, %at, "installing a snapshot in place of the learner's log");
        let work = async {
            replica.begin_install(config.epoch).await?;
            loop {
                let header: Backfill =
                    wire::body(&frame, MessageKind::Backfill).map_err(protocol)?;
                if header.position() != at {
                    return Err(protocol("a snapshot chunk names another position"));
                }
                let rows = SnapshotRows::from_payload(&frame.payload).map_err(protocol)?;
                install_rows(replica, rows.rows).await?;
                if header.done {
                    break;
                }
                frame = recv(receiver, inner.config.link_timeout).await?;
            }
            replica.finish_install(at).await?;
            Ok::<_, LinkError>(())
        };
        match inner.set.reopen_after(replica, &config, &inner.node, work).await {
            Ok((reopened, installed)) => {
                reopened.set_ack_timeout(inner.config.ack_timeout);
                installed.map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        }
    };
    let ack = BackfillAck {
        refused: installed.as_ref().err().cloned().unwrap_or_default(),
        ..BackfillAck::default()
    };
    sender
        .send(&wire::frame(MessageKind::BackfillAck, &ack, Bytes::new()))
        .await?;
    match installed {
        Ok(()) => {
            tracing::info!(%shard, %at, "installed a snapshot");
            Ok(())
        }
        Err(error) => Err(protocol(format_args!("installing a snapshot failed: {error}"))),
    }
}

/// Stores `rows` of a snapshot, a run of rows of one table at a time.
async fn install_rows<D: Disk>(replica: &Shard<D>, rows: Vec<Row>) -> Result<(), LinkError> {
    let mut rows = rows.into_iter().peekable();
    while let Some(first) = rows.peek() {
        let code = first.table;
        let table = ShardTable::from_code(code)
            .ok_or_else(|| protocol(format_args!("a snapshot row of table {code}")))?;
        let mut run = Vec::new();
        while let Some(row) = rows.next_if(|row| row.table == code) {
            run.push((row.key, row.value));
        }
        replica.install_rows(table, run).await?;
    }
    Ok(())
}

/// The primary's backfill of the payload of its learner `learner`, over a
/// connection of its own: see the [module](self) docs. Returns once the
/// learner's backfill is complete.
pub(super) async fn fill<N: Network, D: Disk>(
    replication: &Replication<N, D>,
    shard: &Shard<D>,
    leader: &Leader,
    learner: &NodeId,
) -> Result<(), LinkError> {
    let inner = &*replication.inner;
    let address = inner
        .peers
        .get(learner)
        .ok_or_else(|| protocol(format_args!("no address for {learner}")))?;
    let connection = inner.transport.connect(learner, address).await?;
    let (mut receiver, mut sender) = connection.into_split();
    let open = Backfill {
        config: shard.config().to_json().map_err(protocol)?,
        ..Backfill::default()
    };
    sender
        .send(&wire::frame(MessageKind::Backfill, &open, Bytes::new()))
        .await?;
    let timeout = inner.config.link_timeout * PATIENCE;
    loop {
        let frame = recv(&mut receiver, timeout).await?;
        let ack: BackfillAck = wire::body(&frame, MessageKind::BackfillAck).map_err(protocol)?;
        if !ack.refused.is_empty() {
            return Err(protocol(format_args!("the learner refused: {}", ack.refused)));
        }
        if ack.complete {
            return if leader.fill_complete(learner, ack.applied()) {
                Ok(())
            } else {
                Err(protocol("the learner reported a backfill older than its snapshot"))
            };
        }
        if ack.needs.len() > NEEDS || shard.is_stopped() || !leader.is_learner(learner) {
            return Err(protocol("the backfill is over"));
        }
        for position in ack.positions() {
            let record = shard.payload_record(position).await?;
            let header = Backfill {
                epoch: position.epoch.get(),
                seq: position.seq.get(),
                missing: record.is_none(),
                ..Backfill::default()
            };
            let frame = wire::frame(MessageKind::Backfill, &header, record.unwrap_or_default());
            sender.send(&frame).await?;
        }
    }
}

/// Serves a backfill connection from the primary of one of this node's
/// learners, whose first frame is `first`, until the learner holds every
/// payload its entries need: see the [module](self) docs.
pub(super) async fn serve<S, N, D>(
    (mut receiver, mut sender): (Receiver<S>, Sender<S>),
    first: &Frame,
    replication: &Replication<N, D>,
) -> Result<(), LinkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    N: Network,
    D: Disk,
{
    let inner = &*replication.inner;
    let replica = match learner(first, receiver.peer(), replication).await {
        Ok(replica) => replica,
        Err(refused) => {
            let ack = BackfillAck {
                refused: refused.clone(),
                ..BackfillAck::default()
            };
            sender
                .send(&wire::frame(MessageKind::BackfillAck, &ack, Bytes::new()))
                .await?;
            return Err(LinkError::Protocol(refused));
        }
    };
    let timeout = inner.config.link_timeout * PATIENCE;
    let (mut cursor, mut found, mut lacking) = (FillCursor::default(), 0, false);
    loop {
        if replica.is_stopped() {
            return Err(protocol("the learner's replica stopped"));
        }
        let (missing, next) = replica.missing_payload(cursor).await?;
        found += missing.len();
        for chunk in missing.chunks(NEEDS) {
            let needs = chunk
                .iter()
                .map(|position| Run {
                    epoch: position.epoch.get(),
                    last: position.seq.get(),
                })
                .collect();
            let ask = BackfillAck {
                needs,
                ..BackfillAck::default()
            };
            sender
                .send(&wire::frame(MessageKind::BackfillAck, &ask, Bytes::new()))
                .await?;
            let mut records = Vec::with_capacity(chunk.len());
            for &position in chunk {
                let frame = recv(&mut receiver, timeout).await?;
                let header: Backfill =
                    wire::body(&frame, MessageKind::Backfill).map_err(protocol)?;
                if header.position() != position {
                    return Err(protocol("the primary sent payload of another position"));
                }
                if header.missing {
                    lacking = true;
                } else {
                    records.push((position, frame.payload));
                }
            }
            replica.store_payload(records).await?;
        }
        cursor = match next {
            Some(next) => next,
            None if found == 0 => break,
            None => {
                // The primary lacked payload an entry here names: the
                // entry is older than the primary's, and the live stream
                // brings the newer one.
                if std::mem::take(&mut lacking) {
                    tokio::time::sleep(inner.config.reconnect_delay).await;
                }
                found = 0;
                FillCursor::default()
            }
        };
    }
    let applied = replica.applied();
    let done = BackfillAck {
        complete: true,
        applied_epoch: applied.epoch.get(),
        applied_seq: applied.seq.get(),
        ..BackfillAck::default()
    };
    sender
        .send(&wire::frame(MessageKind::BackfillAck, &done, Bytes::new()))
        .await?;
    tracing::info!(shard = %replica.shard(), %applied, "the learner holds its payload");
    Ok(())
}

/// The learner replica a backfill connection's first frame `first` names,
/// if `peer` is its primary in its epoch and it installed a snapshot or
/// holds a log; otherwise why not.
async fn learner<N: Network, D: Disk>(
    first: &Frame,
    peer: &PeerIdentity,
    replication: &Replication<N, D>,
) -> Result<Shard<D>, String> {
    let request: Backfill = wire::body(first, MessageKind::Backfill)?;
    let theirs = ShardConfig::from_json(&request.config).map_err(|e| e.to_string())?;
    let shard = ShardRef::new(theirs.bucket_id.clone(), theirs.shard);
    let replica = replication
        .inner
        .set
        .get(&shard)
        .await
        .ok_or("the shard is not open on this node")?;
    if !matches!(peer, PeerIdentity::Node(node) if *node == theirs.primary) {
        return Err("the peer is not the primary it names".to_owned());
    }
    if replica.role() != Role::Learner || replica.config() != theirs {
        return Err("this node is not a learner in the primary's configuration".to_owned());
    }
    if replica.applied() == EpochSeq::default() {
        return Err("the learner holds nothing to backfill yet".to_owned());
    }
    Ok(replica)
}
