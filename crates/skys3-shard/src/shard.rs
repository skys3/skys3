//! A shard replica on one node: sequencing, the commit pipeline, and seals
//! (§5.1, §4.1), for the only member of a shard, its primary, or a member
//! that follows the primary.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use skys3_index::{Entry, EntryState, Index, IndexError, ListPage, ListQuery, Part, Upload};
use skys3_io::{BlockingPool, Disk};
use skys3_log::record::truncated_by;
use skys3_log::record::{Extent, ExtentRef, MpuPart, Put, PutData};
use skys3_log::{LogRecord, RecordBody, RecordKind, SegmentClass, SegmentLog, ShardRef};
use skys3_types::{Epoch, EpochSeq, NodeId, Seq, ShardConfig};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::ack::{AckMode, AckTimeout};
use crate::error::ShardError;
use crate::leader::Leader;
use crate::lineage::{Lineage, Reconcile};
use crate::machine::{Effect, Outcome, Recorder, StateMachine};
use crate::pipeline::{Durable, Pipeline, Ready};

mod backfill;

pub(crate) use self::backfill::FillCursor;

/// How many entries a summary reads per index transaction.
const SUMMARY_PAGE: usize = 1024;

/// A record the shard committed: its position and what applying it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// The record's position in the shard's log.
    pub position: EpochSeq,
    /// What applying the record did. A rejected record is committed too: it
    /// is in the log, and every replica rejects it alike.
    pub outcome: Outcome,
}

/// A client write the shard applied, as [`Shard::subscribe`] reports it:
/// a `PUT`, `DELETE`, or `TAGS` that stored a new version of its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// The key written.
    pub key: String,
    /// The position of the record, which is the new version's.
    pub position: EpochSeq,
    /// The new version's size: the object's for a `PUT` or an
    /// `MPU_COMPLETE`, 0 for a `DELETE`, and `None` for a `TAGS`, which
    /// keeps the size it had.
    pub size: Option<u64>,
}

/// What a shard holds, as deleting its bucket needs to know (§4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardSummary {
    /// Keys with a live object: every entry except delete tombstones.
    pub objects: u64,
    /// Entries whose latest change has not reached the bucket's target:
    /// dirty, flushing, and conflicted entries, tombstones included. Nothing
    /// in a `local` bucket is flushed in M1, so every entry of one counts.
    pub unflushed: u64,
}

/// What a replica does in its shard's configuration (§4.1, §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// The only member, and so the primary: a record commits once it is
    /// durable here.
    Alone,
    /// The primary of a configuration with other members: a record commits
    /// once every member has it durably, and reads are served only while
    /// every member grants a lease (§5.4).
    Primary,
    /// A member that stores and acknowledges what its primary sends, and
    /// applies it once the primary's commit watermark covers it. It serves
    /// no client request.
    Member,
    /// A learner (§6.4, §6.7): it follows its primary as a member does,
    /// but the commit rule counts it only while its primary keeps it in
    /// the acknowledgement set, and it never takes over. A promotion makes
    /// it a member.
    Learner,
}

impl Role {
    /// Whether the replica follows a primary: a member or a learner.
    #[must_use]
    pub fn follows(self) -> bool {
        matches!(self, Role::Member | Role::Learner)
    }
}

type WriteReply = oneshot::Sender<Result<Committed, ShardError>>;
type WriteReceiver = oneshot::Receiver<Result<Committed, ShardError>>;
type BarrierReply = oneshot::Sender<Result<(), ShardError>>;

/// What the shard's appender task receives, in position order.
enum Job {
    /// Append `record`, boxed as records are large next to the rest.
    Append {
        record: Box<LogRecord>,
        /// The record as encoded, if it arrived encoded from the primary.
        encoded: Option<Bytes>,
        lazy: bool,
    },
    /// Append `record`, a `TRUNCATE` that no writer waits on, and reply
    /// once it is durable or failed (§6.6).
    Mark {
        record: Box<LogRecord>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Reply once every record queued before is durable or failed.
    Settle(oneshot::Sender<()>),
}

/// What the shard's pipeline task receives.
pub(crate) enum Message {
    /// A record got `position`; its writer waits on `reply`.
    Sequenced {
        position: EpochSeq,
        reply: WriteReply,
    },
    /// The append of the record at `position` finished.
    Appended {
        position: EpochSeq,
        result: Result<Durable, String>,
    },
    /// Reply once every record sequenced before is applied or failed.
    Barrier(BarrierReply),
    /// Every record up to this `seq` is committed.
    Commit(Seq),
    /// Drop the records after `after` unapplied, which a member truncated
    /// with a `TRUNCATE` record at `marker` (§6.6), and release nothing
    /// queued later until that record is durable.
    Truncate { after: Seq, marker: EpochSeq },
    /// The append of the `TRUNCATE` record at `position` finished. If it
    /// failed, the shard stops.
    Marked {
        position: EpochSeq,
        result: Result<(), String>,
    },
    /// Stop the shard, fail every record and barrier still queued, then
    /// reply.
    Abandon(BarrierReply),
}

/// The sequencing state, changed only under its lock.
#[derive(Debug)]
pub(crate) struct Sequencer {
    /// The position the next record gets.
    pub(crate) next: EpochSeq,
    /// The last position applied to the index.
    pub(crate) applied: EpochSeq,
    /// What this replica does in the shard.
    role: Role,
    /// Whether the replica serves client requests: always as the only
    /// member, never as a member, and as a primary once its members hold
    /// its whole log (§6.6).
    pub(crate) serving: bool,
    /// A member's current session with its primary: appends of an older
    /// session are refused.
    session: u64,
    /// How many seals are held (§4.1).
    seals: u32,
    /// The newest configuration of the shard the replica knows. `next` is
    /// in its epoch once the replica has appended its `CONFIG` record; until
    /// then the replica aligns with its members or its primary (see
    /// [`Shard::follow`] and [`Shard::reconfigure`]).
    config: ShardConfig,
    /// The node the replica runs on, if it was opened for a named node.
    node: Option<NodeId>,
    /// On a member that is not aligned, whose primary sequences in the
    /// newest configuration's epoch already: the `seq` of the primary's
    /// `CONFIG` record, unless a record of the new epoch shows it first.
    adopt_after: Option<Seq>,
    /// Why the shard stopped, once it has.
    stopped: Option<String>,
    /// Whether the replica stopped as a primary that learned another one
    /// took over (§6.5): it then redirects to the configuration it learned.
    deposed: bool,
    /// The epoch in which the replica stepped down as primary for a
    /// planned handoff (§5.4): it serves nothing from then on.
    pub(crate) stepped_down: Option<Epoch>,
    /// The configuration this member last proposed to take the shard over
    /// in, in this life or, durably, an earlier one (§6.3). It is settled
    /// once the replica's configuration reaches its epoch, or forgotten
    /// once the proposal lost.
    takeover: Option<ShardConfig>,
    /// The epochs of the records the replica's log holds (§6.6).
    lineage: Lineage,
    /// The position of the latest record sequenced for each key, other than
    /// an `EXTENT`, while it is not applied, and while a conditional read
    /// that began at or before it is in progress (see
    /// [`Shard::commit_if`]).
    writes: BTreeMap<String, EpochSeq>,
    /// Conditional reads in progress, counted by the position the next
    /// record had when each began.
    readers: BTreeMap<EpochSeq, usize>,
    /// Where applied client writes are reported, if anyone subscribed.
    subscriber: Option<mpsc::UnboundedSender<Change>>,
    /// How long requests wait for the members, on a replicated shard.
    ack: Option<AckTimeout>,
    /// In fail-fast mode, the last position sequenced when a request
    /// last timed out: new writes are refused until it is applied.
    late: Option<EpochSeq>,
    /// How many learners a primary's acknowledgement set holds: they count
    /// as acknowledging copies for `min_write_replicas` (§6.4).
    pub(crate) acking: usize,
}

struct Inner<D: Disk> {
    shard: ShardRef,
    log: SegmentLog<D>,
    index: Arc<Index>,
    pool: BlockingPool,
    sequencer: Arc<Mutex<Sequencer>>,
    pipeline: mpsc::UnboundedSender<Message>,
    appender: mpsc::UnboundedSender<Job>,
    /// The last `seq` of the longest run of the shard's records that is
    /// durable here.
    durable: watch::Receiver<Seq>,
    /// The primary's replication state, once the replica is a primary.
    leader: Arc<OnceLock<Arc<Leader>>>,
    /// How many reads of the index are in progress, admitted while the
    /// replica served reads.
    reads: watch::Sender<usize>,
}

/// One shard replica on this node: the shard's only member, its primary,
/// or a member (see [`Role`]).
///
/// - **Sequencing.** Each record gets the next position `(epoch, seq)` of
///   the shard's log. It is checked before it gets one, so a record the
///   log would refuse never leaves a gap. A member takes the positions its
///   primary gave ([`Shard::receive`]).
/// - **Appending.** Records are queued to the log in position order. On a
///   replicated shard, a record of the other segment class is queued only
///   once every earlier record is durable, so the replica's log never
///   holds a record without every earlier one (§10.1).
/// - **Commit.** With one member, a record commits once the log has made
///   it durable. A primary sends each record to every member as it appends
///   it, and the record commits once every member has acknowledged it as
///   durable too ([`Leader`]); a member applies records once the primary's
///   commit watermark covers them (§5.1). Records become durable out of
///   order, so a pipeline applies them to the index in position order: a
///   record is applied, and its writer answered, only once every earlier
///   record is durable, committed, and applied. An `EXTENT` is applied like
///   any record, so a `PUT` that references extents is accepted only once
///   they are applied.
/// - **Failure.** A record that cannot be made durable stops the shard: it
///   and every later record fail, and the shard refuses writes until the
///   node reopens it after replaying its log. Its disk is out of service by
///   then anyway (§10.4).
/// - **Seals** ([`Shard::seal`], [`Shard::unseal`]) are ordered with the
///   shard's writes, as deleting a bucket requires (§4.1).
/// - **Configurations** ([`Shard::reconfigure`]) change in order with the
///   writes too: a `CONFIG` record at `(epoch, last seq)` switches the
///   replica to the new epoch, at the same `seq` on every replica of a
///   replicated shard (§5.1). A replicated shard changes by removing
///   members (§6.4), and refuses client writes while it has fewer members
///   than its `min_write_replicas`.
///
/// Cloning a shard returns another handle to the same replica.
pub struct Shard<D: Disk> {
    inner: Arc<Inner<D>>,
}

impl<D: Disk> Clone for Shard<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: Disk> fmt::Debug for Shard<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shard")
            .field("shard", &self.inner.shard)
            .field("sequencer", &*self.sequencer())
            .finish_non_exhaustive()
    }
}

impl<D: Disk> Shard<D> {
    /// Opens the shard of `config` on this node and starts its pipeline on
    /// the current Tokio runtime.
    ///
    /// The index must already hold every durable record of the shard: open
    /// shards only after [`Checkpointer::replay`](skys3_index::Checkpointer::replay).
    /// If `config` is a newer epoch than the shard's applied position, the
    /// replica adopts it first: it appends and applies a `CONFIG` record at
    /// `(epoch, last seq)` before it sequences anything in the epoch
    /// (§10.1). Sequencing continues from the last applied `seq`.
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if `config` has more than one member
    /// or any learner (see [`Shard::open_replica`]), or is older than an
    /// epoch the shard has applied; [`ShardError::Unavailable`] if the
    /// index or the log fails.
    pub async fn open(
        config: &ShardConfig,
        log: SegmentLog<D>,
        index: Arc<Index>,
        pool: BlockingPool,
    ) -> Result<Self, ShardError> {
        Self::open_as(config, None, log, index, pool).await
    }

    /// Opens the replica of node `node` in the shard of `config`, as
    /// [`Shard::open`] does, in the [`Role`] the configuration gives it.
    ///
    /// A primary with other members starts without serving: its
    /// replication links (see `Replication`) first bring every member's log
    /// to its own, rolling forward records a member holds beyond it, and the
    /// primary serves once all of them are committed (§6.6). A member's
    /// next record is the one after the last it holds, and it applies
    /// records as the primary's commit watermark reaches them. A replica's
    /// log has no holes, so the records it holds, committed or not, are a
    /// prefix of the primary's.
    ///
    /// A replica whose log holds records of an older epoch than `config`'s,
    /// after its shard changed configuration while it was down, opens in
    /// its log's epoch ([`Shard::sequencing`]) and switches to `config`'s at
    /// the same `seq` as its primary: a member as its primary's sessions
    /// show it ([`Shard::follow`]), a primary once every member has reported
    /// its log ([`Shard::align`]).
    ///
    /// A learner whose log holds nothing of the shard yet opens in no epoch,
    /// and takes its primary's log from the first record on, each record in
    /// the epoch it was sequenced in, switching to `config`'s as a member
    /// does (§6.7).
    ///
    /// A primary that stepped down in `config`'s epoch for a planned
    /// handoff, in an earlier life too, opens stopped: it never serves in
    /// that epoch again (§5.4, [`Shard::stepped_down`]). A primary that
    /// recorded a promotion it proposed in an earlier life, and whose
    /// epoch `config` has not reached, keeps waiting for that learner until
    /// it learns the outcome (§6.3, [`Leader::promoting`]). Likewise a member
    /// that recorded a takeover over `config` in an earlier life follows no
    /// primary of `config`'s epoch until it learns the outcome
    /// ([`Shard::outstanding_takeover`]).
    ///
    /// Each `CONFIG` record the replica applies is kept in the index as
    /// the shard's configuration on this node, the local copy a restarted
    /// node opens the replica in while the register cannot be read
    /// ([`ShardSet::kept_config`](crate::ShardSet::kept_config), §6.2).
    ///
    /// # Errors
    ///
    /// As [`Shard::open`], and [`ShardError::Configuration`] if `node` is
    /// neither a member nor a learner of `config`.
    pub async fn open_replica(
        config: &ShardConfig,
        node: &NodeId,
        log: SegmentLog<D>,
        index: Arc<Index>,
        pool: BlockingPool,
    ) -> Result<Self, ShardError> {
        Self::open_as(config, Some(node), log, index, pool).await
    }

    async fn open_as(
        config: &ShardConfig,
        node: Option<&NodeId>,
        log: SegmentLog<D>,
        index: Arc<Index>,
        pool: BlockingPool,
    ) -> Result<Self, ShardError> {
        let shard = ShardRef::new(config.bucket_id.clone(), config.shard);
        let role = check_config(&shard, config, node)?;
        let (applied, stepped_down, promotion, takeover) = {
            let (index, key) = (Arc::clone(&index), shard.clone());
            run(&pool, &shard, move || {
                let read = index.read()?;
                Ok((
                    read.applied(&key)?,
                    read.step_down(&key)?,
                    read.promotion(&key)?,
                    read.takeover(&key)?,
                ))
            })
            .await?
        };
        let applied = match applied {
            // A snapshot install was cut short (§6.7): what it stored goes,
            // and the learner holds nothing until its next snapshot.
            Some(marker) if marker.seq == Seq::MAX => {
                if role != Role::Learner {
                    return Err(ShardError::configuration(
                        &shard,
                        "a snapshot install did not finish, and the replica is not a learner",
                    ));
                }
                let (index, key) = (Arc::clone(&index), shard.clone());
                run(&pool, &shard, move || index.begin_install(&key, marker)).await?;
                None
            }
            applied => applied,
        };
        let epoch = config.epoch;
        // A primary that stepped down in this epoch never serves in it
        // again, even after a restart: its step-down message may still
        // reach the candidate (§5.4).
        let stepped_down =
            stepped_down.filter(|stepped| role == Role::Primary && *stepped == epoch);
        // A takeover this member proposed over `config` in an earlier life,
        // whose outcome it has not learned: it may have made the node the
        // primary, so the member acknowledges nothing in this epoch until
        // it knows (§6.3).
        let takeover = takeover.filter(|proposed| {
            role == Role::Member && proposed.epoch > epoch && node == Some(&proposed.primary)
        });
        // A replicated replica that holds records of an older epoch cannot
        // tell alone where the new epoch starts: a member learns it from
        // its primary's session, and a primary appends its `CONFIG` record
        // once every member has reported its log (§5.1). Both open in the
        // epoch of their last record, and align later.
        // A primary with no other member has no one to align with. A
        // learner that holds nothing yet takes the primary's log from its
        // first record, in the epochs it was sequenced in.
        let aligns = role.follows() || (role == Role::Primary && config.members.len() > 1);
        let aligning = (aligns && applied.is_some_and(|a| a.epoch < epoch))
            || (role == Role::Learner && applied.is_none());
        let mut last = match applied {
            Some(applied) if applied.epoch > epoch => {
                return Err(ShardError::configuration(
                    &shard,
                    format!("epoch {epoch} is older than the applied position {applied}"),
                ));
            }
            Some(applied) => applied,
            None => EpochSeq::new(Epoch::ZERO, Seq::ZERO),
        };
        if !aligning && applied.is_none_or(|applied| applied.epoch < epoch) {
            let record = LogRecord {
                shard: shard.clone(),
                position: EpochSeq::new(epoch, last.seq),
                body: RecordBody::Config(config.clone()),
            };
            let location = log
                .append(&record)
                .await
                .map_err(|error| ShardError::unavailable(&shard, error))?;
            last = record.position;
            let index = Arc::clone(&index);
            run(&pool, &shard, move || {
                index.apply(&StateMachine, &[(record, location)])
            })
            .await?;
        }
        let next = last
            .seq
            .checked_next()
            .ok_or_else(|| ShardError::configuration(&shard, "the shard's seq is exhausted"))?;
        let sequencer = Arc::new(Mutex::new(Sequencer {
            next: EpochSeq::new(last.epoch, next),
            applied: last,
            role,
            serving: role == Role::Alone,
            session: 0,
            seals: 0,
            config: config.clone(),
            node: node.cloned(),
            adopt_after: None,
            stopped: stepped_down.map(|_| STEPPED_DOWN.to_owned()),
            deposed: false,
            stepped_down,
            takeover,
            lineage: Lineage::new(last),
            writes: BTreeMap::new(),
            readers: BTreeMap::new(),
            subscriber: None,
            ack: None,
            late: None,
            acking: 0,
        }));
        let (sender, receiver) = mpsc::unbounded_channel();
        let (durable_tx, durable) = watch::channel(last.seq);
        let leader = Arc::new(OnceLock::new());
        if role == Role::Primary {
            // A promotion this primary proposed in an earlier life, whose
            // outcome it may not know: it keeps waiting for the learner.
            let _ = leader.set(Arc::new(Leader::new(
                shard.clone(),
                config,
                promotion.as_ref(),
                Arc::clone(&sequencer),
                sender.clone(),
                durable.clone(),
            )));
        }
        let mut pipeline = Pipeline::default();
        if role != Role::Alone {
            pipeline.require_commit(last.seq);
        }
        let task = PipelineTask {
            shard: shard.clone(),
            index: Arc::clone(&index),
            pool: pool.clone(),
            sequencer: Arc::clone(&sequencer),
            pipeline,
            durable: durable_tx,
            leader: Arc::clone(&leader),
            stopped: None,
        };
        tokio::spawn(task.run(receiver));
        let (appender, jobs) = mpsc::unbounded_channel();
        let task = Appender {
            log: log.clone(),
            pipeline: sender.clone(),
            leader: Arc::clone(&leader),
            ordered: role != Role::Alone,
            in_flight: JoinSet::new(),
            class: SegmentClass::Hot,
        };
        tokio::spawn(task.run(jobs));
        Ok(Self {
            inner: Arc::new(Inner {
                shard,
                log,
                index,
                pool,
                sequencer,
                pipeline: sender,
                appender,
                durable,
                leader,
                reads: watch::Sender::new(0),
            }),
        })
    }

    /// The shard.
    #[must_use]
    pub fn shard(&self) -> &ShardRef {
        &self.inner.shard
    }

    /// The index the shard applies its records to.
    #[must_use]
    pub fn index(&self) -> &Arc<Index> {
        &self.inner.index
    }

    /// The position of the last record applied to the index.
    #[must_use]
    pub fn applied(&self) -> EpochSeq {
        self.sequencer().applied
    }

    /// The newest configuration of the shard this replica knows. It
    /// sequences in it once [`Shard::sequencing`] reaches its epoch.
    #[must_use]
    pub fn config(&self) -> ShardConfig {
        self.sequencer().config.clone()
    }

    /// The epoch the replica sequences records in: its configuration's,
    /// unless it still aligns with its primary or members after learning a
    /// newer one (§5.1).
    #[must_use]
    pub fn sequencing(&self) -> Epoch {
        self.sequencer().next.epoch
    }

    /// Whether any seal is held.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sequencer().seals > 0
    }

    /// What this replica does in the shard.
    #[must_use]
    pub fn role(&self) -> Role {
        self.sequencer().role
    }

    /// Whether the replica serves client requests (see [`Role`]): a primary
    /// with other members serves once it has reconciled them.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.sequencer().serving
    }

    /// A primary's replication state, which its replication links drive.
    #[must_use]
    pub fn leader(&self) -> Option<&Arc<Leader>> {
        self.inner.leader.get()
    }

    /// The epochs of the records the replica's log holds, as far as this
    /// life knows them (§6.6).
    #[must_use]
    pub fn lineage(&self) -> Lineage {
        self.sequencer().lineage.clone()
    }

    /// Bounds how long the requests of a replicated shard wait for its
    /// members (§5.2): writes, seals, conditional checks that wait for an
    /// earlier write of their key, and [`Shard::close`]. A request that
    /// waits longer fails with [`ShardError::NotAcknowledged`]; its record,
    /// if it got one, keeps its position and may still commit. A shard
    /// alone waits only for its disk, so this changes nothing on it.
    pub fn set_ack_timeout(&self, ack: AckTimeout) {
        let mut sequencer = self.sequencer();
        if sequencer.role != Role::Alone {
            sequencer.ack = Some(ack);
        }
    }

    /// The shard's [`AckTimeout`], if it has one.
    #[must_use]
    pub fn ack_timeout(&self) -> Option<AckTimeout> {
        self.sequencer().ack
    }

    /// The last `seq` of the longest run of the shard's records that is
    /// durable on this replica, committed or not, as it changes.
    #[must_use]
    pub fn durable(&self) -> watch::Receiver<Seq> {
        self.inner.durable.clone()
    }

    /// The `seq` of the last record sequenced, durable or not.
    #[must_use]
    pub fn last_sequenced(&self) -> Seq {
        self.sequencer().last_sequenced().seq
    }

    /// Commits `body` as the shard's next record, and returns its position
    /// and what applying it did, once it is durable and applied.
    ///
    /// Client writes (`PUT`, `DELETE`, `TAGS`, `EXTENT`, and the multipart
    /// records) are refused while the shard is sealed; `FLUSHED`, `IMPORT`,
    /// and `ADOPT` are not. A `PUT` or `MPU_PART` that references extents is
    /// accepted only once each of them is applied: append them with
    /// [`Shard::append_extent`] first (§10.1).
    ///
    /// The record is appended even if the returned future is dropped, or
    /// the write times out.
    ///
    /// # Errors
    ///
    /// [`ShardError::Sealed`]; [`ShardError::InvalidRecord`] if the record
    /// cannot be encoded, references an extent that is not applied, or is a
    /// `CONFIG` or `TRUNCATE`, which a replica appends for itself;
    /// [`ShardError::NotAcknowledged`] if the members did not acknowledge
    /// it within the shard's [`AckTimeout`], or a late write makes a
    /// fail-fast shard refuse it; and [`ShardError::Unavailable`] if the
    /// shard stopped.
    pub async fn commit(&self, body: RecordBody) -> Result<Committed, ShardError> {
        let (position, reply) = self.submit_locked(&mut self.sequencer(), body, false)?;
        self.within_ack(self.answer(reply))
            .await
            .map_err(|error| error.sequenced_at(position))
    }

    /// Commits `body` like [`Shard::commit`], but lets its record ride the
    /// next group commit of the shard's disk instead of starting one
    /// ([`SegmentLog::append_lazy`]). The flusher commits `FLUSHED` records
    /// this way, so they never cost a sync of their own (§7.1).
    ///
    /// Records after it are applied only after it, so it can delay their
    /// answers by at most [`skys3_log::LAZY_MAX_DELAY`], and only while
    /// nothing else is written to the disk.
    ///
    /// # Errors
    ///
    /// As [`Shard::commit`].
    pub async fn commit_lazy(&self, body: RecordBody) -> Result<Committed, ShardError> {
        let (position, reply) = self.submit_locked(&mut self.sequencer(), body, true)?;
        self.within_ack(self.answer(reply))
            .await
            .map_err(|error| error.sequenced_at(position))
    }

    /// The answer to a record's writer.
    async fn answer(&self, reply: WriteReceiver) -> Result<Committed, ShardError> {
        reply
            .await
            .unwrap_or_else(|_| Err(self.unavailable("the shard's pipeline stopped")))
    }

    /// Waits for `request` for at most the shard's [`AckTimeout`], if it
    /// has one. A request that waits longer fails as not acknowledged, and
    /// in fail-fast mode makes the shard refuse new writes until every
    /// record sequenced so far is applied.
    async fn within_ack<T>(
        &self,
        request: impl Future<Output = Result<T, ShardError>>,
    ) -> Result<T, ShardError> {
        let ack = self.sequencer().ack;
        let Some(ack) = ack else {
            return request.await;
        };
        match tokio::time::timeout(ack.timeout, request).await {
            Ok(result) => result,
            Err(_) => {
                let mut sequencer = self.sequencer();
                if ack.mode == AckMode::FailFast {
                    let last = sequencer.last_sequenced();
                    sequencer.late = sequencer.late.max(Some(last));
                }
                Err(ShardError::not_acknowledged(
                    self.shard(),
                    format!(
                        "its members did not acknowledge it within {:?}",
                        ack.timeout
                    ),
                ))
            }
        }
    }

    /// Commits one extent of a large body (§5.1), and returns the reference
    /// a `PUT` uses once the extent is durable and applied.
    ///
    /// # Errors
    ///
    /// As [`Shard::commit`].
    pub async fn append_extent(&self, extent: Extent) -> Result<ExtentRef, ShardError> {
        let len = u32::try_from(extent.data.len())
            .map_err(|_| ShardError::invalid(self.shard(), "the extent is too long"))?;
        let committed = self.commit(RecordBody::Extent(extent)).await?;
        Ok(ExtentRef {
            position: committed.position,
            len,
        })
    }

    /// Sequences `body` and starts its append, lazily if `lazy`, under the
    /// sequencer's lock, which the caller holds, so records reach the
    /// pipeline in position order. Returns the record's position.
    fn submit_locked(
        &self,
        sequencer: &mut Sequencer,
        body: RecordBody,
        lazy: bool,
    ) -> Result<(EpochSeq, WriteReceiver), ShardError> {
        let shard = self.shard();
        sequencer.check_serving(shard)?;
        sequencer.check_late(shard)?;
        if matches!(body, RecordBody::Config(_) | RecordBody::Truncate) {
            return Err(ShardError::invalid(
                shard,
                "CONFIG and TRUNCATE records are appended by the replica itself",
            ));
        }
        let client_write = is_client_write(&body);
        if client_write && sequencer.seals > 0 {
            return Err(ShardError::Sealed(shard.clone()));
        }
        // A shard left with fewer members than `min_write_replicas` stays
        // readable and refuses client writes (§6.4).
        if client_write && let Some(error) = sequencer.under_replicated(shard) {
            return Err(error);
        }
        let data = match &body {
            RecordBody::Put(Put { data, .. }) | RecordBody::MpuPart(MpuPart { data, .. }) => {
                Some(data)
            }
            _ => None,
        };
        if let Some(PutData::Extents(extents)) = data
            && let Some(extent) = extents.iter().find(|e| e.position > sequencer.applied)
        {
            return Err(ShardError::invalid(
                shard,
                format!("the extent at {} is not applied", extent.position),
            ));
        }
        let position = sequencer.next;
        let next = position
            .seq
            .checked_next()
            .ok_or_else(|| ShardError::invalid(shard, "the shard's seq is exhausted"))?;
        // A `FLUSHED` changes no object version, so conditional checks
        // need not wait for it (see `commit_if`).
        let key = entry_key(&body)
            .filter(|_| !matches!(body, RecordBody::Flushed(_)))
            .map(str::to_owned);
        let receiver = self.sequence(sequencer, position, body, None, lazy)?;
        sequencer.next = EpochSeq::new(position.epoch, next);
        if let Some(key) = key {
            sequencer.writes.insert(key, position);
        }
        Ok((position, receiver))
    }

    /// Takes `record`, which the primary of `epoch` sequenced and encoded
    /// as `encoded`, as this replica's next record, and starts its append,
    /// lazily if `lazy` (see [`Shard::commit_lazy`]). A member applies it
    /// once the primary's commit watermark covers it
    /// ([`Shard::commit_through`]); the [`Shard::durable`] watch shows when
    /// it is durable.
    ///
    /// A member passes the session its primary's link opened
    /// ([`Shard::begin_session`]), so that an append from an older session,
    /// such as one still in flight from before the primary restarted, is
    /// refused. A record this replica already holds is skipped, so a
    /// primary can roll forward records several members send it (§6.6).
    /// Returns whether the record was taken.
    ///
    /// A record must be of the epoch the replica sequences in
    /// ([`Shard::sequencing`]). A member that still sequences in an earlier
    /// epoch than its configuration's takes that epoch's tail, and appends
    /// its `CONFIG` record just before the first record of the new epoch,
    /// or once it reaches its primary's `CONFIG` record (see
    /// [`Shard::follow`]), so that it switches epochs at the same `seq` as
    /// its primary (§5.1). A primary that took over may also send records
    /// of epochs between the two, which it rolls forward from earlier
    /// primaries (§6.6): the replica sequences in each as it comes.
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if `epoch` is not the epoch of the
    /// shard's configuration (rule R2 refuses older ones; a newer one needs
    /// the configuration first), or the session is not the current one;
    /// [`ShardError::InvalidRecord`] if the record is of another shard, is a
    /// `CONFIG` or `TRUNCATE`, which a replica appends for itself, is of
    /// another epoch than the one the replica sequences in, cannot be
    /// appended, or does not follow the last record held;
    /// [`ShardError::Unavailable`] if the shard stopped.
    pub fn receive(
        &self,
        session: Option<u64>,
        epoch: Epoch,
        record: LogRecord,
        encoded: Bytes,
        lazy: bool,
    ) -> Result<bool, ShardError> {
        let shard = self.shard();
        let mut sequencer = self.sequencer();
        sequencer.check_running(shard)?;
        if session.is_some_and(|session| session != sequencer.session) {
            return Err(ShardError::configuration(
                shard,
                "the append is of an earlier session with the primary",
            ));
        }
        let ours = sequencer.config.epoch;
        if epoch != ours {
            return Err(ShardError::configuration(
                shard,
                format!("the append is of epoch {epoch}, the replica's is {ours}"),
            ));
        }
        if record.shard != *shard
            || matches!(record.kind(), RecordKind::Config | RecordKind::Truncate)
        {
            return Err(ShardError::invalid(
                shard,
                format!(
                    "a replica cannot take a {:?} of shard {}",
                    record.kind(),
                    record.shard
                ),
            ));
        }
        let position = record.position;
        let next = sequencer.next.seq;
        // A replica still in an earlier epoch switches at the first record
        // of its configuration's epoch: the primary that sequenced it
        // appended its CONFIG record at the `seq` before. A member whose
        // primary's CONFIG record is earlier than that holds records of
        // the earlier epoch its primary does not, and never switches.
        if !sequencer.is_aligned()
            && position.epoch == ours
            && position.seq == next
            && sequencer
                .adopt_after
                .is_none_or(|after| sequencer.last_sequenced().seq <= after)
        {
            drop(self.adopt_locked(&mut sequencer)?);
        }
        // The record's own epoch, not only the message's: a record of
        // another epoch would move the sequencer away from the
        // configuration. Only the tail of the epoch a replica still
        // sequences in, and of later epochs before its configuration's,
        // can arrive in a session of a newer one (§5.1, §6.6).
        let sequencing = sequencer.next.epoch;
        let later = position.epoch > sequencing && position.epoch < ours && position.seq == next;
        if position.epoch != sequencing && !later {
            return Err(ShardError::invalid(
                shard,
                format!("the record at {position} is not of the replica's epoch {sequencing}"),
            ));
        }
        match position.seq.cmp(&next) {
            Ordering::Less => return Ok(false),
            Ordering::Greater => {
                return Err(ShardError::invalid(
                    shard,
                    format!("the record at {position} does not follow seq {next}"),
                ));
            }
            Ordering::Equal => {}
        }
        let after = next
            .checked_next()
            .ok_or_else(|| ShardError::invalid(shard, "the shard's seq is exhausted"))?;
        let body = record.body;
        drop(self.sequence(&mut sequencer, position, body, Some(encoded), lazy)?);
        sequencer.next = EpochSeq::new(position.epoch, after);
        self.adopt_if_due(&mut sequencer)?;
        Ok(true)
    }

    /// Tells a member how far its primary has come, as the primary's
    /// session reported it: the epoch the primary sequences in, and its
    /// last `seq`. A member whose configuration is newer than the epoch it
    /// sequences in, and whose primary sequences in that configuration's
    /// epoch already, appends its own `CONFIG` record at the `seq` of the
    /// primary's: before the first record of the new epoch, or once it
    /// holds the primary's last record if all of them are of the earlier
    /// epoch (§5.1). Anything else changes nothing.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard stopped, and
    /// [`ShardError::InvalidRecord`] if the `CONFIG` record cannot be
    /// appended.
    pub fn follow(&self, sequencing: Epoch, primary_last: Seq) -> Result<(), ShardError> {
        let mut sequencer = self.sequencer();
        sequencer.check_running(self.shard())?;
        if !sequencer.role.follows() || sequencer.is_aligned() {
            return Ok(());
        }
        sequencer.adopt_after = (sequencing == sequencer.config.epoch).then_some(primary_last);
        self.adopt_if_due(&mut sequencer)
    }

    /// Appends a member's `CONFIG` record once it holds every record up to
    /// its primary's `CONFIG` record; the caller holds the sequencer's
    /// lock. A member that holds records past it diverged from its primary
    /// and never adopts: it refuses the new epoch's records, and its
    /// primary removes it (§6.4).
    fn adopt_if_due(&self, sequencer: &mut Sequencer) -> Result<(), ShardError> {
        let due = sequencer.role.follows()
            && !sequencer.is_aligned()
            && sequencer.adopt_after == Some(sequencer.last_sequenced().seq);
        if due {
            drop(self.adopt_locked(sequencer)?);
        }
        Ok(())
    }

    /// Appends the `CONFIG` record of the replica's newest configuration at
    /// `(epoch, last seq)`, after every record sequenced so far, and
    /// sequences later records in its epoch (§10.1); the caller holds the
    /// sequencer's lock. Returns what the record's writer waits on.
    fn adopt_locked(&self, sequencer: &mut Sequencer) -> Result<WriteReceiver, ShardError> {
        let config = sequencer.config.clone();
        let position = EpochSeq::new(config.epoch, sequencer.last_sequenced().seq);
        let reply = self.sequence(sequencer, position, RecordBody::Config(config), None, false)?;
        sequencer.next = EpochSeq::new(position.epoch, sequencer.next.seq);
        sequencer.adopt_after = None;
        Ok(reply)
    }

    /// Appends a primary's `CONFIG` record of its newest configuration, if
    /// it does not sequence in it yet and every member of it has reported
    /// its log in this life, so that no member holds a record of the
    /// earlier epoch past it (§5.1). A primary's links call it as members
    /// report. Returns whether it appended the record.
    pub fn align(&self) -> bool {
        self.align_now().is_some()
    }

    /// [`Shard::align`], returning what the record's writer waits on, if
    /// the record was appended.
    fn align_now(&self) -> Option<WriteReceiver> {
        let leader = self.leader()?;
        let config = self.config();
        if !leader.synced_all(&config) {
            return None;
        }
        let mut sequencer = self.sequencer();
        if sequencer.config != config || sequencer.is_aligned() || sequencer.stopped.is_some() {
            return None;
        }
        match self.adopt_locked(&mut sequencer) {
            Ok(reply) => Some(reply),
            Err(error) => {
                tracing::warn!(shard = %self.shard(), %error, "appending a CONFIG record failed");
                None
            }
        }
    }

    /// Starts a member's new session with its primary, after which appends
    /// of earlier sessions are refused ([`Shard::receive`]), and returns
    /// it.
    pub fn begin_session(&self) -> u64 {
        let mut sequencer = self.sequencer();
        sequencer.session += 1;
        sequencer.session
    }

    /// Returns once every record sequenced so far is durable or failed, so
    /// that [`Shard::durable`] then shows every record this replica will
    /// ever hold from before the call.
    pub async fn settle(&self) {
        let (reply, settled) = oneshot::channel();
        if self.inner.appender.send(Job::Settle(reply)).is_ok() {
            // The appender ends only with the shard.
            let _ = settled.await;
        }
    }

    /// Lets the replica apply every durable record up to `seq`: a member
    /// learns the commit watermark from its primary (§5.1). A lower `seq`
    /// than before changes nothing.
    pub fn commit_through(&self, seq: Seq) {
        // The pipeline is gone only if the shard is.
        let _ = self.inner.pipeline.send(Message::Commit(seq));
    }

    /// What this member does with its log when the primary of a
    /// configuration in `epoch`, whose log has the lineage `primary`,
    /// opens a session: see [`Lineage::reconcile`] (§6.6). Call it once
    /// the records queued so far are durable ([`Shard::settle`]).
    #[must_use]
    pub fn reconcile(&self, primary: &Lineage, epoch: Epoch) -> Reconcile {
        let sequencer = self.sequencer();
        sequencer
            .lineage
            .reconcile(primary, epoch, sequencer.applied.seq)
    }

    /// Invalidates this member's records after `after`, which its new
    /// primary does not hold, with a `TRUNCATE` record at
    /// `(epoch, after)`, and returns once that record is durable (§6.6).
    /// The member then takes the primary's records from `after + 1` on.
    ///
    /// The truncated records were never committed, since committing them
    /// needed the new primary's acknowledgement. They are dropped from the
    /// pipeline unapplied, replay and [`Shard::read_tail`] skip them
    /// ([`truncated_by`]), and the commit watermark of the earlier primary
    /// stops counting past `after`. `epoch` must be newer than every record
    /// truncated and no newer than any taken next, as [`Shard::reconcile`]
    /// chooses it.
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] on a replica that is not a member, or
    /// if a record past `after` is applied already;
    /// [`ShardError::Unavailable`] if the shard stopped, or the `TRUNCATE`
    /// record did not become durable, which stops it.
    pub async fn truncate(&self, after: Seq, epoch: Epoch) -> Result<(), ShardError> {
        let shard = self.shard();
        self.settle().await;
        let (reply, durable) = oneshot::channel();
        {
            let mut sequencer = self.sequencer();
            sequencer.check_running(shard)?;
            if !sequencer.role.follows() {
                return Err(ShardError::configuration(
                    shard,
                    "only a member or a learner truncates",
                ));
            }
            if after < sequencer.applied.seq {
                return Err(ShardError::configuration(
                    shard,
                    format!("records up to seq {} are applied", sequencer.applied.seq),
                ));
            }
            if after >= sequencer.last_sequenced().seq {
                return Ok(());
            }
            let marker = EpochSeq::new(epoch, after);
            let record = LogRecord {
                shard: shard.clone(),
                position: marker,
                body: RecordBody::Truncate,
            };
            // The pipeline drops the records before any later one reaches
            // it. The TRUNCATE is never applied, since its position is past
            // records it leaves valid, but nothing queued after it is
            // applied, or counts as durable, until it is durable.
            let stopped = || self.unavailable("the shard's pipeline stopped");
            self.inner
                .pipeline
                .send(Message::Truncate { after, marker })
                .map_err(|_| stopped())?;
            let job = Job::Mark {
                record: Box::new(record),
                reply,
            };
            self.inner.appender.send(job).map_err(|_| stopped())?;
            sequencer.lineage.truncate(after);
            let epoch = sequencer.lineage.last().epoch;
            let next = after.checked_next().unwrap_or(Seq::MAX);
            sequencer.next = EpochSeq::new(epoch, next);
            sequencer.adopt_after = None;
            sequencer.writes.retain(|_, position| position.seq <= after);
        }
        let failed = match durable.await {
            Ok(Ok(())) => return self.sequencer().check_running(shard),
            Ok(Err(error)) => format!("a TRUNCATE record did not become durable: {error}"),
            Err(_) => "the shard's appender stopped".to_owned(),
        };
        // The pipeline stops the shard too; stop it now, so that no record
        // takes a truncated one's place without the TRUNCATE.
        self.sequencer()
            .stopped
            .get_or_insert_with(|| failed.clone());
        Err(self.unavailable(&failed))
    }

    /// Steps down as the primary for a planned handoff to the member `to`
    /// (§5.4), and returns the epoch it steps down in and the last `seq` it
    /// sequenced: what the step-down message tells the candidate.
    ///
    /// The replica stops serving reads and writes at once, and stops
    /// renewing its leases. Reads it admitted before finish, and the
    /// writes it sequenced commit as far as the members acknowledge them
    /// within the shard's [`AckTimeout`], so their writers are answered.
    /// It then records the step-down durably, as it must never serve in
    /// this epoch again, even after a restart (see [`Shard::open`]), and
    /// only then owes the candidate the message, which the link to `to`
    /// sends after the last record ([`Leader`]).
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if the replica is not a primary that
    /// leads the shard, or `to` is not another member of it;
    /// [`ShardError::Unavailable`] if the shard stopped, or the step-down
    /// could not be recorded: the replica then serves nothing and owes no
    /// message, so the members take over once their grace passes.
    pub(crate) async fn step_down(&self, to: &NodeId) -> Result<(Epoch, Seq), ShardError> {
        let shard = self.shard();
        let (epoch, barrier) = {
            let mut sequencer = self.sequencer();
            sequencer.check_running(shard)?;
            let config = &sequencer.config;
            if sequencer.role != Role::Primary || sequencer.stepped_down.is_some() {
                return Err(ShardError::configuration(
                    shard,
                    "only a primary that leads the shard hands it off",
                ));
            }
            if *to == config.primary || !config.is_member(to) {
                return Err(ShardError::configuration(
                    shard,
                    format!("{to} is not another member of the shard"),
                ));
            }
            let epoch = config.epoch;
            sequencer.stepped_down = Some(epoch);
            sequencer.serving = false;
            (epoch, self.barrier())
        };
        let Some(leader) = self.leader() else {
            unreachable!("a primary has a leader");
        };
        leader.stop_leases();
        let mut reads = self.inner.reads.subscribe();
        // The sender lives as long as the shard.
        let _ = reads.wait_for(|reads| *reads == 0).await;
        if let Err(error) = self.within_ack(barrier).await {
            tracing::info!(%shard, %error, "handing off before every write committed");
        }
        let last = self.sequencer().last_sequenced().seq;
        let (index, key) = (Arc::clone(&self.inner.index), shard.clone());
        let stored = run(&self.inner.pool, shard, move || {
            index.store_step_down(&key, epoch)
        })
        .await;
        if let Err(error) = stored {
            self.depose("the step-down could not be recorded", None);
            return Err(error);
        }
        leader.owe_step_down(to.clone(), epoch, last);
        Ok((epoch, last))
    }

    /// The epoch in which the replica stepped down as primary for a
    /// planned handoff, in this life or, durably, an earlier one (§5.4).
    #[must_use]
    pub fn stepped_down(&self) -> Option<Epoch> {
        self.sequencer().stepped_down
    }

    /// Records durably that this primary proposes `config` to promote a
    /// learner (§6.3): a primary that opens again before `config`'s epoch
    /// keeps waiting for that learner ([`Shard::open_replica`]).
    pub(crate) async fn record_promotion(&self, config: &ShardConfig) -> Result<(), ShardError> {
        let (index, config) = (Arc::clone(&self.inner.index), config.clone());
        run(&self.inner.pool, self.shard(), move || {
            index.store_promotion(&config)
        })
        .await
    }

    /// Records durably that this member proposes `config` to take the
    /// shard over (§6.3, §6.5): a member that opens again before `config`'s
    /// epoch acknowledges nothing in its epoch until it learns the outcome
    /// ([`Shard::outstanding_takeover`]).
    pub(crate) async fn record_takeover(&self, config: &ShardConfig) -> Result<(), ShardError> {
        let (index, stored) = (Arc::clone(&self.inner.index), config.clone());
        run(&self.inner.pool, self.shard(), move || {
            index.store_takeover(&stored)
        })
        .await?;
        self.sequencer().takeover = Some(config.clone());
        Ok(())
    }

    /// Forgets the takeover this member recorded, once it learned that the
    /// proposal lost. Returns whether it did: while the record stays, the
    /// member must grant nothing, since a restart would send the proposal
    /// again.
    pub(crate) async fn forget_takeover(&self) -> bool {
        let (index, key) = (Arc::clone(&self.inner.index), self.shard().clone());
        let forgotten = run(&self.inner.pool, self.shard(), move || {
            index.forget_takeover(&key)
        })
        .await;
        match forgotten {
            Ok(()) => {
                self.sequencer().takeover = None;
                true
            }
            Err(error) => {
                tracing::warn!(shard = %self.shard(), %error, "cannot forget a takeover that lost");
                false
            }
        }
    }

    /// The configuration this member proposed to take the shard over in,
    /// in this life or an earlier one, while its outcome is not settled:
    /// the replica's configuration has not reached its epoch (§6.3). Until
    /// the member learns the outcome, it follows no primary in an older
    /// epoch, since the proposal may have made it the primary.
    #[must_use]
    pub fn outstanding_takeover(&self) -> Option<ShardConfig> {
        let sequencer = self.sequencer();
        sequencer
            .takeover
            .clone()
            .filter(|proposed| proposed.epoch > sequencer.config.epoch)
    }

    /// Makes this member the primary of `config`, which it proposed over
    /// its current configuration `over` and the shard's register accepted
    /// (§6.5). It appends `config`'s `CONFIG` record after every record it
    /// holds, and returns once that record is durable; it serves once its
    /// links have reconciled every member with its log and all of it is
    /// committed (§6.6). Its records past the commit watermark it knew stay
    /// in the pipeline and commit under `config`.
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if the replica is not a member in
    /// `over`, or `config` does not name this node as its primary in a
    /// newer epoch; [`ShardError::Unavailable`] if the shard stopped.
    pub(crate) async fn take_over(
        &self,
        over: &ShardConfig,
        config: &ShardConfig,
    ) -> Result<(), ShardError> {
        let shard = self.shard();
        self.settle().await;
        {
            let mut sequencer = self.sequencer();
            sequencer.check_running(shard)?;
            let ours = sequencer.node.as_ref() == Some(&config.primary);
            if sequencer.role != Role::Member
                || sequencer.config != *over
                || !ours
                || config.epoch <= over.epoch
            {
                return Err(ShardError::configuration(
                    shard,
                    "the replica is not the member that proposed the configuration",
                ));
            }
            let leader = Leader::new(
                shard.clone(),
                config,
                None,
                Arc::clone(&self.inner.sequencer),
                self.inner.pipeline.clone(),
                self.inner.durable.clone(),
            );
            if self.inner.leader.set(Arc::new(leader)).is_err() {
                return Err(ShardError::configuration(shard, "the replica led before"));
            }
            sequencer.role = Role::Primary;
            sequencer.config = config.clone();
            sequencer.serving = false;
            sequencer.adopt_after = None;
            // Appends of the earlier primary's sessions are refused.
            sequencer.session += 1;
            drop(self.adopt_locked(&mut sequencer)?);
        }
        // Members reconcile against a log that holds the CONFIG record.
        self.settle().await;
        self.sequencer().check_running(shard)
    }

    /// The records of the shard after `after`, up to `through`, as this
    /// replica's log holds them, encoded, in `seq` order: what a primary
    /// sends a member that is behind, or a member sends a primary that
    /// restarted behind it (§6.6).
    ///
    /// It reads every segment of the shard's disk, so it is for catching
    /// up after a restart, not for the write path.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the log fails, or does not hold every
    /// record in the range.
    pub async fn read_tail(
        &self,
        after: Seq,
        through: Seq,
    ) -> Result<Vec<(EpochSeq, Bytes)>, ShardError> {
        let shard = self.shard();
        let log = &self.inner.log;
        let failed = |error: &dyn fmt::Display| ShardError::unavailable(shard, error);
        let (mut candidates, mut truncates) = (Vec::new(), Vec::new());
        for segment in log.segments() {
            let mut scanner = log.scan(segment.id).map_err(|e| failed(&e))?;
            while let Some(record) = scanner.next().await.map_err(|e| failed(&e))? {
                let header = &record.header;
                let seq = header.position.seq;
                if header.shard != *shard {
                    continue;
                }
                match header.kind {
                    RecordKind::Truncate => truncates.push(header.position),
                    RecordKind::Config => {}
                    _ if seq > after && seq <= through => {
                        candidates.push((header.position, record.bytes));
                    }
                    _ => {}
                }
            }
        }
        // Records a reconciliation truncated are not the shard's (§6.6).
        let found: BTreeMap<_, _> = candidates
            .into_iter()
            .filter(|(position, _)| !truncates.iter().any(|t| truncated_by(*t, *position)))
            .map(|(position, bytes)| (position.seq, (position, bytes)))
            .collect();
        let expected = through.get().saturating_sub(after.get());
        if found.len() as u64 != expected {
            return Err(failed(&format!(
                "the log holds {} of the records after seq {after} through {through}",
                found.len()
            )));
        }
        Ok(found.into_values().collect())
    }

    /// Commits `body`, a record that names a key, if `check` accepts the
    /// key's entry as it is when the record is sequenced: the outcome of
    /// every record of the key sequenced before it, applied or not. If
    /// `check` refuses, nothing is appended and its error is returned.
    ///
    /// The check and the record's position are therefore linearizable with
    /// every other write of the key, as conditional requests need (§5.1).
    /// A `FLUSHED` record is the exception: it changes the entry's state and
    /// remote fields but never its object version, which is all a
    /// conditional request checks, so the check does not wait for it.
    /// While a record of the key is sequenced but not yet applied, the check
    /// waits until it is. `check` may run more than once, and runs under the
    /// sequencer's lock, so it must be quick.
    ///
    /// # Errors
    ///
    /// The outer error as [`Shard::commit`], or
    /// [`ShardError::InvalidRecord`] for a record that names no key or is an
    /// `EXTENT`; the inner one is `check`'s. The shard's [`AckTimeout`]
    /// bounds the whole request, waits for earlier writes of the key
    /// included.
    pub async fn commit_if<E>(
        &self,
        body: RecordBody,
        check: impl FnMut(Option<&Entry>) -> Result<(), E>,
    ) -> Result<Result<Committed, E>, ShardError> {
        let sequenced = OnceLock::new();
        self.within_ack(self.check_and_commit(body, check, &sequenced))
            .await
            .map_err(|error| match sequenced.get() {
                Some(&position) => error.sequenced_at(position),
                None => error,
            })
    }

    /// [`Shard::commit_if`] without the timeout. It sets `sequenced` to the
    /// record's position once it has one.
    async fn check_and_commit<E>(
        &self,
        body: RecordBody,
        mut check: impl FnMut(Option<&Entry>) -> Result<(), E>,
        sequenced: &OnceLock<EpochSeq>,
    ) -> Result<Result<Committed, E>, ShardError> {
        let key = entry_key(&body)
            .ok_or_else(|| ShardError::invalid(self.shard(), "the record names no entry"))?
            .to_owned();
        let mut body = Some(body);
        loop {
            // Records of the key sequenced before the read must be applied,
            // so that the index holds their outcome.
            let begun = {
                let mut sequencer = self.sequencer();
                sequencer.check_serving(self.shard())?;
                match sequencer.writes.get(&key) {
                    Some(&position) if position > sequencer.applied => Err(self.barrier()),
                    _ => Ok(sequencer.begin_read()),
                }
            };
            let since = match begun {
                Ok(since) => since,
                Err(barrier) => {
                    barrier.await?;
                    continue;
                }
            };
            let read = ReadGuard {
                shard: self,
                since: Some(since),
            };
            let entry = self.entry(&key).await?;
            let reply = {
                let mut sequencer = self.sequencer();
                // A record of the key sequenced since the read began is
                // still listed while the read lasts, and the read may have
                // missed it.
                let raced = sequencer.writes.get(&key).is_some_and(|&p| p >= since);
                read.end(&mut sequencer);
                if raced {
                    continue;
                }
                if let Err(error) = check(entry.as_ref()) {
                    return Ok(Err(error));
                }
                let Some(body) = body.take() else {
                    unreachable!("the record is sequenced at most once");
                };
                let (position, reply) = self.submit_locked(&mut sequencer, body, false)?;
                let _ = sequenced.set(position);
                reply
            };
            return self.answer(reply).await.map(Ok);
        }
    }

    /// The entry of `key` as the index holds it: the outcome of every
    /// applied record of the key, and of no record not yet applied.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and as
    /// [`Shard::check_readable`].
    pub async fn entry(&self, key: &str) -> Result<Option<Entry>, ShardError> {
        let _read = self.admit_read()?;
        let (index, shard, key) = (
            Arc::clone(&self.inner.index),
            self.shard().clone(),
            key.to_owned(),
        );
        run(&self.inner.pool, self.shard(), move || {
            index.read()?.entry(&shard, &key)
        })
        .await
    }

    /// One page of the shard's listing (§9.4), as the index holds it: the
    /// outcome of every applied record, and so of every acknowledged write.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and as
    /// [`Shard::check_readable`].
    pub async fn list(&self, query: ListQuery) -> Result<ListPage, ShardError> {
        let _read = self.admit_read()?;
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            index.read()?.list(&shard, &query)
        })
        .await
    }

    /// The open upload of `key` opened at `upload`, as the index holds it,
    /// with up to `limit` of its parts after part `after` (0 for the first).
    /// The upload and its parts are read at one point in the shard's
    /// history, so an upload that is open never comes with the parts of a
    /// later abort or completion.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and as
    /// [`Shard::check_readable`].
    pub async fn upload(
        &self,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<(Upload, Vec<(u16, Part)>)>, ShardError> {
        let _read = self.admit_read()?;
        let (index, shard, key) = (
            Arc::clone(&self.inner.index),
            self.shard().clone(),
            key.to_owned(),
        );
        run(&self.inner.pool, self.shard(), move || {
            let reader = index.read()?;
            let Some(state) = reader.upload(&shard, &key, upload)? else {
                return Ok(None);
            };
            let parts = if limit == 0 {
                Vec::new()
            } else {
                reader.parts(&shard, upload, after, limit)?
            };
            Ok(Some((state, parts)))
        })
        .await
    }

    /// Up to `limit` open uploads whose keys start with `prefix`, by key and
    /// age, after `after` (see
    /// [`IndexReader::uploads`](skys3_index::IndexReader::uploads)).
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and as
    /// [`Shard::check_readable`].
    pub async fn uploads(
        &self,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        let _read = self.admit_read()?;
        let (index, shard, prefix) = (
            Arc::clone(&self.inner.index),
            self.shard().clone(),
            prefix.to_owned(),
        );
        run(&self.inner.pool, self.shard(), move || {
            let after = after.as_ref().map(|(key, upload)| (key.as_str(), *upload));
            index.read()?.uploads(&shard, &prefix, after, limit)
        })
        .await
    }

    /// Up to `limit` parts of the upload opened at `upload`, open or
    /// completed, in part order after part `after` (0 for the first), all
    /// read at one point in the shard's history.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and as
    /// [`Shard::check_readable`].
    pub async fn parts(
        &self,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        let _read = self.admit_read()?;
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            index.read()?.parts(&shard, upload, after, limit)
        })
        .await
    }

    /// The payload of the record at `position`, as an entry or a part names
    /// it: an `EXTENT`'s data, or the inline bytes of a `PUT` or an
    /// `MPU_PART`.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if this replica locates no payload at
    /// `position`, or the index or the log fails.
    pub async fn payload(&self, position: EpochSeq) -> Result<Bytes, ShardError> {
        let shard = self.shard();
        let location = {
            let (index, key) = (Arc::clone(&self.inner.index), shard.clone());
            run(&self.inner.pool, shard, move || {
                index.read()?.location(&key, position)
            })
            .await?
        };
        let Some(location) = location else {
            return Err(self.unavailable(&format!("no payload is located at {position}")));
        };
        let record = self
            .inner
            .log
            .read(location)
            .await
            .map_err(|error| ShardError::unavailable(shard, error))?;
        match backfill::payload_of(&record) {
            Some(data) if record.shard == *shard && record.position == position => Ok(data.clone()),
            _ => Err(self.unavailable(&format!("the record at {position} holds no payload"))),
        }
    }

    /// Adopts `config`, a newer epoch of the shard, in order with its
    /// writes.
    ///
    /// The `CONFIG` record takes the position `(epoch, last seq)`, after
    /// every record sequenced so far, and every later record is sequenced in
    /// the new epoch (§10.1). As every record, it is applied, and the writers
    /// of the records after it answered, only once it is durable, so nothing
    /// is acknowledged in the new epoch before the configuration is. The
    /// configuration the shard is in already changes nothing, even on a
    /// stopped shard.
    ///
    /// On a replicated shard the configuration removes members (§6.4),
    /// names one of them as the new primary after a takeover (§6.5), which
    /// a member follows, adds or removes learners, or promotes a learner to
    /// member (§6.7), which it then is. The epoch must switch at the same
    /// `seq` on every replica (§5.1), so:
    ///
    /// - **A primary** appends the record once every member of `config`
    ///   has reported its log in this life, at once if they have, and
    ///   returns once it is durable and applied; otherwise it returns at
    ///   once, and its link to the last member to report appends it. The
    ///   members `config` leaves out stop counting for commits and leases
    ///   once the record is durable here ([`Leader`]), so the records they
    ///   held back commit under the new epoch.
    /// - **A member or a learner** returns at once, and appends the record
    ///   where its primary's session shows the primary's is
    ///   ([`Shard::follow`]). Meanwhile it refuses appends of the older
    ///   epoch (rule R2).
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if `config` is of another shard, is
    /// older than the shard's configuration, differs from it in the same
    /// epoch, cannot open (see [`Shard::open`]), or makes a change a
    /// replicated shard does not support: a new primary on the primary or
    /// on the member it names, a member that was not a learner, a member
    /// made a learner, or this replica's removal; on a shard opened alone,
    /// any other member or learner;
    /// [`ShardError::Unavailable`] if the shard stopped, and otherwise as
    /// [`Shard::commit`].
    pub async fn reconfigure(&self, config: &ShardConfig) -> Result<(), ShardError> {
        match self.begin_reconfigure(config)? {
            Some(reply) => reply
                .await
                .unwrap_or_else(|_| Err(self.unavailable("the shard's pipeline stopped")))
                .map(drop),
            None => Ok(()),
        }
    }

    /// [`Shard::reconfigure`] up to its `CONFIG` record's append: returns
    /// what the record's writer waits on, if the replica appended it now.
    pub(crate) fn begin_reconfigure(
        &self,
        config: &ShardConfig,
    ) -> Result<Option<WriteReceiver>, ShardError> {
        let shard = self.shard();
        let reply = {
            let mut sequencer = self.sequencer();
            let target = ShardRef::new(config.bucket_id.clone(), config.shard);
            if target != *shard {
                return Err(ShardError::configuration(
                    shard,
                    format!("the configuration is of shard {target}"),
                ));
            }
            let epoch = sequencer.config.epoch;
            match config.epoch.cmp(&epoch) {
                Ordering::Less => {
                    return Err(ShardError::configuration(
                        shard,
                        format!(
                            "epoch {} is older than the shard's epoch {epoch}",
                            config.epoch
                        ),
                    ));
                }
                Ordering::Equal if *config == sequencer.config => return Ok(None),
                Ordering::Equal => {
                    return Err(ShardError::configuration(
                        shard,
                        format!("the configuration differs from the shard's in epoch {epoch}"),
                    ));
                }
                Ordering::Greater => {}
            }
            sequencer.check_running(shard)?;
            if sequencer.role == Role::Alone {
                if check_config(shard, config, sequencer.node.as_ref())? != Role::Alone {
                    return Err(ShardError::configuration(
                        shard,
                        "a shard opened alone takes members and learners only once it \
                         reopens as a primary",
                    ));
                }
                sequencer.config = config.clone();
                Some(self.adopt_locked(&mut sequencer)?)
            } else {
                sequencer.check_change(shard, config)?;
                // A promotion (§6.7): the learner is a member from now on.
                if sequencer.role == Role::Learner
                    && sequencer.node.as_ref().is_some_and(|n| config.is_member(n))
                {
                    sequencer.role = Role::Member;
                }
                sequencer.config = config.clone();
                sequencer.adopt_after = None;
                None
            }
        };
        Ok(reply.or_else(|| self.align_now()))
    }

    /// Stops a primary that learned it no longer leads the shard: its
    /// register holds a configuration it cannot adopt, `register` if the
    /// register holds one, or a member refused its appends for a newer
    /// epoch (§6.5). Its writers still waiting are not acknowledged. It
    /// serves nothing from then on: with the register's configuration it
    /// answers requests with that configuration as a redirect hint
    /// ([`ShardError::NotPrimary`]), and without it as unavailable.
    pub(crate) fn depose(&self, reason: &str, register: Option<&ShardConfig>) {
        let mut sequencer = self.sequencer();
        sequencer.stopped.get_or_insert_with(|| reason.to_owned());
        sequencer.subscriber = None;
        sequencer.deposed = true;
        if let Some(config) = register.filter(|config| config.epoch > sequencer.config.epoch) {
            sequencer.config = config.clone();
        }
    }

    /// Whether the replica was a primary that another one replaced
    /// (§6.5).
    #[must_use]
    pub fn is_deposed(&self) -> bool {
        self.sequencer().deposed
    }

    /// Gives `body` the position `position` and queues its append, as
    /// `encoded` if it arrived encoded; the caller holds the sequencer's
    /// lock, so records reach the pipeline and the appender in position
    /// order, and advances `next` on success. The lineage records the
    /// position.
    fn sequence(
        &self,
        sequencer: &mut Sequencer,
        position: EpochSeq,
        body: RecordBody,
        encoded: Option<Bytes>,
        lazy: bool,
    ) -> Result<WriteReceiver, ShardError> {
        let shard = self.shard();
        let record = LogRecord {
            shard: shard.clone(),
            position,
            body,
        };
        self.inner
            .log
            .check(&record)
            .map_err(|error| ShardError::invalid(shard, error))?;
        let (reply, receiver) = oneshot::channel();
        let stopped = || self.unavailable("the shard's pipeline stopped");
        self.inner
            .pipeline
            .send(Message::Sequenced { position, reply })
            .map_err(|_| stopped())?;
        let job = Job::Append {
            record: Box::new(record),
            encoded,
            lazy,
        };
        self.inner.appender.send(job).map_err(|_| stopped())?;
        sequencer.lineage.push(position);
        Ok(receiver)
    }

    /// Up to `limit` entries of the shard in key order, after `start_after`
    /// if given, tombstones included, as the index holds them.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn entries(
        &self,
        start_after: Option<String>,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ShardError> {
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            index.read()?.entries(&shard, start_after.as_deref(), limit)
        })
        .await
    }

    /// Reports every client write applied from now on: each `PUT`,
    /// `DELETE`, and `TAGS` that stored a new version, in position order.
    /// The shard's flusher follows it (§7.1). A shard has one subscriber; a
    /// new subscription replaces the previous one, whose stream then ends.
    /// The stream also ends when the shard stops.
    ///
    /// A write is either in the index when `subscribe` returns or reported
    /// to the new subscriber, also while it is being applied, so a scan
    /// that starts after `subscribe` returns, together with the stream,
    /// misses none. A write may show in both.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<Change> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut sequencer = self.sequencer();
        if sequencer.stopped.is_none() {
            sequencer.subscriber = Some(sender);
        }
        receiver
    }

    /// Whether the shard stopped: it was closed, or a record failed. A
    /// stopped shard refuses every request until the node reopens it.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.sequencer().stopped.is_some()
    }

    /// Seals the shard and reports what it holds (§4.1).
    ///
    /// Once `seal` returns, no client write commits until every seal is
    /// lifted, and the summary counts every write committed before it.
    /// Seals nest: each needs its own [`Shard::unseal`]. If `seal` fails, or
    /// its future is dropped before it returns, the seal is lifted again.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard stopped or its index fails;
    /// [`ShardError::NotAcknowledged`] if the writes before the seal did not
    /// commit within the shard's [`AckTimeout`].
    pub async fn seal(&self) -> Result<ShardSummary, ShardError> {
        let barrier = {
            let mut sequencer = self.sequencer();
            sequencer.check_running(self.shard())?;
            sequencer.seals += 1;
            self.barrier()
        };
        let mut guard = SealGuard(Some(self));
        self.within_ack(barrier).await?;
        let summary = self.summary().await?;
        guard.0 = None;
        Ok(summary)
    }

    /// Lifts one seal. Lifting a seal that is not held changes nothing.
    pub fn unseal(&self) {
        let mut sequencer = self.sequencer();
        sequencer.seals = sequencer.seals.saturating_sub(1);
    }

    /// Stops the shard once every record sequenced so far is applied or
    /// failed: later requests fail with [`ShardError::Unavailable`].
    ///
    /// A replicated shard waits for that at most its [`AckTimeout`], since
    /// a member that does not acknowledge would hold it forever (§5.2).
    /// Then it stops all the same, and abandons the records still waiting
    /// for the members: their writers, who have timed out by then, were not
    /// acknowledged, nothing applies the records here any more, and they
    /// stay in the log for the shard's next opening to commit or discard.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard had stopped already.
    pub async fn close(&self) -> Result<(), ShardError> {
        let (barrier, ack) = {
            let mut sequencer = self.sequencer();
            sequencer.check_running(self.shard())?;
            sequencer.stopped = Some("the shard was closed".to_owned());
            sequencer.subscriber = None;
            (self.barrier(), sequencer.ack)
        };
        let Some(ack) = ack else {
            return barrier.await;
        };
        if let Ok(closed) = tokio::time::timeout(ack.timeout, barrier).await {
            return closed;
        }
        tracing::warn!(
            shard = %self.shard(),
            timeout = ?ack.timeout,
            "closing the shard abandons the records its members did not acknowledge",
        );
        let (reply, abandoned) = oneshot::channel();
        if self.inner.pipeline.send(Message::Abandon(reply)).is_ok() {
            // The pipeline answers every waiter before it replies, and
            // ends only with the shard.
            let _ = abandoned.await;
        }
        Ok(())
    }

    /// Queues a barrier; the caller holds the sequencer's lock.
    fn barrier(&self) -> impl Future<Output = Result<(), ShardError>> + use<D> {
        let (reply, receiver) = oneshot::channel();
        let sent = self.inner.pipeline.send(Message::Barrier(reply)).is_ok();
        let error = self.unavailable("the shard's pipeline stopped");
        async move {
            if !sent {
                return Err(error);
            }
            receiver.await.unwrap_or(Err(error))
        }
    }

    /// Counts the shard's entries, as of the writes applied so far.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn summary(&self) -> Result<ShardSummary, ShardError> {
        self.fold_entries(ShardSummary::default(), |mut summary, entry| {
            summary.objects += u64::from(entry.object.is_some());
            summary.unflushed += u64::from(!matches!(
                entry.state,
                EntryState::Clean | EntryState::Evicted
            ));
            summary
        })
        .await
    }

    /// The bytes of the object versions the shard holds, as of the writes
    /// applied so far: what has fewer copies than `replicas` while the
    /// shard has fewer members (`under_replicated_bytes`, §6.4).
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn stored_bytes(&self) -> Result<u64, ShardError> {
        self.fold_entries(0, |bytes: u64, entry| {
            bytes.saturating_add(entry.object.as_ref().map_or(0, |object| object.size))
        })
        .await
    }

    /// Folds every entry of the shard into `init`, a page of entries per
    /// index transaction.
    async fn fold_entries<T: Send + 'static>(
        &self,
        init: T,
        fold: impl Fn(T, &Entry) -> T + Send + 'static,
    ) -> Result<T, ShardError> {
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            let reader = index.read()?;
            let mut folded = init;
            let mut after = None;
            loop {
                let page = reader.entries(&shard, after.as_deref(), SUMMARY_PAGE)?;
                for (_, entry) in &page {
                    folded = fold(folded, entry);
                }
                match page.into_iter().last() {
                    Some((key, _)) => after = Some(key),
                    None => return Ok(folded),
                }
            }
        })
        .await
    }

    /// Checks that the replica serves reads: it has not stopped, since its
    /// index may then miss records it acknowledged, it is not a member, nor
    /// a primary still reconciling its members, and as a primary it holds a
    /// valid lease from every member (§5.4).
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard stopped, or is a primary
    /// that does not serve yet or holds no lease from some member, and
    /// [`ShardError::NotPrimary`] on a member.
    pub fn check_readable(&self) -> Result<(), ShardError> {
        self.sequencer().check_serving(self.shard())?;
        match self.leader() {
            Some(leader) if !leader.holds_leases() => {
                Err(self.unavailable("the primary does not hold a lease from every member"))
            }
            _ => Ok(()),
        }
    }

    /// Admits a read of the index, as [`Shard::check_readable`] allows,
    /// and counts it until the returned permit drops, so that a primary
    /// stepping down knows when the reads it admitted are over (§5.4).
    ///
    /// The final check and the count happen under one hold of the
    /// sequencer's lock, which [`Shard::step_down`] takes to stop admitting
    /// reads: a read is either refused or counted before the step-down
    /// looks at the count, never admitted in between.
    pub(crate) fn admit_read(&self) -> Result<ReadPermit<'_>, ShardError> {
        self.check_readable()?;
        let sequencer = self.sequencer();
        // A step-down may have come since.
        sequencer.check_serving(self.shard())?;
        self.inner.reads.send_modify(|reads| *reads += 1);
        drop(sequencer);
        Ok(ReadPermit(&self.inner.reads))
    }

    fn sequencer(&self) -> MutexGuard<'_, Sequencer> {
        lock(&self.inner.sequencer)
    }

    fn unavailable(&self, reason: &str) -> ShardError {
        ShardError::unavailable(self.shard(), reason)
    }
}

/// Why a primary that stepped down for a planned handoff refuses requests.
const STEPPED_DOWN: &str = "the primary stepped down for a planned handoff";

/// A read of the index in progress ([`Shard::admit_read`]).
pub(crate) struct ReadPermit<'a>(&'a watch::Sender<usize>);

impl Drop for ReadPermit<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|reads| *reads -= 1);
    }
}

/// Lifts a seal unless disarmed, so a failed or abandoned `seal` leaves
/// none behind.
struct SealGuard<'a, D: Disk>(Option<&'a Shard<D>>);

impl<D: Disk> Drop for SealGuard<'_, D> {
    fn drop(&mut self) {
        if let Some(shard) = self.0 {
            shard.unseal();
        }
    }
}

impl Sequencer {
    fn check_running(&self, shard: &ShardRef) -> Result<(), ShardError> {
        match &self.stopped {
            Some(reason) => Err(ShardError::unavailable(shard, reason)),
            None => Ok(()),
        }
    }

    /// Checks that the replica serves client requests (see
    /// [`Shard::check_readable`]).
    fn check_role(&self, shard: &ShardRef) -> Result<(), ShardError> {
        match self.role {
            Role::Member | Role::Learner => Err(ShardError::NotPrimary {
                shard: shard.clone(),
                primary: self.config.primary.clone(),
                epoch: self.config.epoch,
            }),
            _ if !self.serving => Err(ShardError::unavailable(
                shard,
                "the primary is bringing its members up to its log",
            )),
            _ => Ok(()),
        }
    }

    /// Checks that the replica takes client writes. A deposed primary that
    /// knows its successor's configuration redirects to it.
    fn check_serving(&self, shard: &ShardRef) -> Result<(), ShardError> {
        if self.deposed && self.node.as_ref() != Some(&self.config.primary) {
            return Err(ShardError::NotPrimary {
                shard: shard.clone(),
                primary: self.config.primary.clone(),
                epoch: self.config.epoch,
            });
        }
        if self.stepped_down.is_some() {
            return Err(ShardError::unavailable(shard, STEPPED_DOWN));
        }
        self.check_running(shard)?;
        self.check_role(shard)
    }

    /// Whether the replica sequences in its newest configuration's epoch:
    /// it appended that configuration's `CONFIG` record.
    pub(crate) fn is_aligned(&self) -> bool {
        self.next.epoch == self.config.epoch
    }

    /// The error a client write gets while the configuration's members and
    /// the learners in the acknowledgement set, the acknowledging copies,
    /// are fewer than its `min_write_replicas` (§6.4), or `None`.
    fn under_replicated(&self, shard: &ShardRef) -> Option<ShardError> {
        let config = &self.config;
        let copies = config.members.len() + self.acking;
        (copies < usize::from(config.min_write_replicas)).then(|| ShardError::UnderReplicated {
            shard: shard.clone(),
            copies,
            min_write_replicas: config.min_write_replicas,
        })
    }

    /// Checks that a replicated shard supports changing to `config`, a
    /// newer configuration: members leave (§6.4), learners come and go,
    /// learners become members (§6.7), and this replica stays a member or
    /// a learner, never moving from member to learner. A new primary is one
    /// of the members, and only a member that does not propose it follows
    /// it (§6.5): a primary learns of its successor by being deposed, and a
    /// candidate takes over itself ([`Shard::take_over`]).
    fn check_change(&self, shard: &ShardRef, config: &ShardConfig) -> Result<(), ShardError> {
        let refuse = |reason: &str| Err(ShardError::configuration(shard, reason));
        if config.primary != self.config.primary
            && (self.role != Role::Member || self.node.as_ref() == Some(&config.primary))
        {
            return refuse("a new primary takes over by proposing itself");
        }
        if !config.is_member(&config.primary) {
            return refuse("the primary is not a member");
        }
        let known = |m: &NodeId| self.config.is_member(m) || self.config.is_learner(m);
        if config.members.iter().any(|m| !known(m)) {
            return refuse("a member joins only as a learner");
        }
        match &self.node {
            Some(node) if !config.is_member(node) && !config.is_learner(node) => {
                refuse("this node is not a member or a learner")
            }
            Some(node) if self.config.is_member(node) && config.is_learner(node) => {
                refuse("a member does not become a learner")
            }
            _ => Ok(()),
        }
    }

    /// Checks that no write is late on a fail-fast shard: one timed out,
    /// and a record sequenced before it is not applied yet.
    fn check_late(&mut self, shard: &ShardRef) -> Result<(), ShardError> {
        match self.late {
            Some(late) if late > self.applied => Err(ShardError::not_acknowledged(
                shard,
                format!("the record at {late} still waits for its members"),
            )),
            Some(_) => {
                self.late = None;
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// The position of the last record sequenced.
    fn last_sequenced(&self) -> EpochSeq {
        // `next.seq` is at least 1: opening starts it after the CONFIG
        // record at `seq` 0 or later.
        EpochSeq::new(self.next.epoch, Seq::new(self.next.seq.get() - 1))
    }

    /// Registers a conditional read, which needs every record of its key
    /// sequenced from now on to stay in `writes` until the read ends, and
    /// returns the position it began at.
    fn begin_read(&mut self) -> EpochSeq {
        *self.readers.entry(self.next).or_default() += 1;
        self.next
    }

    fn end_read(&mut self, since: EpochSeq) {
        if let Some(count) = self.readers.get_mut(&since) {
            *count -= 1;
            if *count == 0 {
                self.readers.remove(&since);
            }
        }
        self.forget_applied();
    }

    /// Forgets the writes that are applied and that no conditional read in
    /// progress may have missed.
    fn forget_applied(&mut self) {
        let applied = self.applied;
        let oldest = self.readers.keys().next().copied();
        self.writes
            .retain(|_, position| *position > applied || oldest.is_some_and(|o| *position >= o));
    }
}

/// Whether `body` is a client write: what seals and too few members refuse
/// (§4.1, §6.4). `FLUSHED`, `IMPORT`, and `ADOPT` are not.
fn is_client_write(body: &RecordBody) -> bool {
    matches!(
        body,
        RecordBody::Put(_)
            | RecordBody::Delete(_)
            | RecordBody::Tags(_)
            | RecordBody::Extent(_)
            | RecordBody::MpuCreate(_)
            | RecordBody::MpuPart(_)
            | RecordBody::MpuComplete(_)
            | RecordBody::MpuAbort(_)
    )
}

/// The key of the entry `body` changes: every record that names a key
/// except an `EXTENT`, which changes only the location map, and the
/// multipart records other than `MPU_COMPLETE`, which change only uploads.
fn entry_key(body: &RecordBody) -> Option<&str> {
    match body {
        RecordBody::Extent(_)
        | RecordBody::MpuCreate(_)
        | RecordBody::MpuPart(_)
        | RecordBody::MpuAbort(_) => None,
        other => other.key(),
    }
}

/// The change a client write would report once applied, by position.
fn change(record: &LogRecord) -> Option<(EpochSeq, Change)> {
    let (key, size) = match &record.body {
        RecordBody::Put(put) => (&put.key, Some(put.size)),
        RecordBody::MpuComplete(complete) => (&complete.key, Some(complete.size)),
        RecordBody::Delete(delete) => (&delete.key, Some(0)),
        RecordBody::Tags(tags) => (&tags.key, None),
        _ => return None,
    };
    let change = Change {
        key: key.clone(),
        position: record.position,
        size,
    };
    Some((record.position, change))
}

/// Sends the subscriber each change whose record stored a new version.
fn report(
    subscriber: &mpsc::UnboundedSender<Change>,
    outcomes: &BTreeMap<EpochSeq, Outcome>,
    changes: &mut BTreeMap<EpochSeq, Change>,
) {
    for (position, outcome) in outcomes {
        let stored = matches!(
            outcome,
            Outcome::Applied(Effect::Stored { .. } | Effect::Tombstoned { .. })
        );
        if let Some(change) = changes.remove(position).filter(|_| stored) {
            // A subscriber that went away needs nothing.
            let _ = subscriber.send(change);
        }
    }
}

/// Ends a conditional read, also when its future is dropped.
struct ReadGuard<'a, D: Disk> {
    shard: &'a Shard<D>,
    since: Option<EpochSeq>,
}

impl<D: Disk> ReadGuard<'_, D> {
    /// Ends the read; the caller holds the sequencer's lock.
    fn end(mut self, sequencer: &mut Sequencer) {
        if let Some(since) = self.since.take() {
            sequencer.end_read(since);
        }
    }
}

impl<D: Disk> Drop for ReadGuard<'_, D> {
    fn drop(&mut self) {
        if let Some(since) = self.since.take() {
            self.shard.sequencer().end_read(since);
        }
    }
}

/// Checks that this build can serve `config`, and returns the role it
/// gives `node`. Without a node, the configuration must have a single
/// member and no learner.
fn check_config(
    shard: &ShardRef,
    config: &ShardConfig,
    node: Option<&NodeId>,
) -> Result<Role, ShardError> {
    let refuse = |reason: &str| Err(ShardError::configuration(shard, reason));
    if !config.is_member(&config.primary) {
        return refuse("the primary is not a member");
    }
    let alone = config.members.len() == 1 && config.learners.is_empty();
    match node {
        None if alone => Ok(Role::Alone),
        None => refuse("a replicated shard opens on a named node"),
        Some(node) if config.is_learner(node) => Ok(Role::Learner),
        Some(node) if !config.is_member(node) => refuse("this node is not a member or a learner"),
        Some(_) if alone => Ok(Role::Alone),
        Some(node) if *node == config.primary => Ok(Role::Primary),
        Some(_) => Ok(Role::Member),
    }
}

fn lock(sequencer: &Mutex<Sequencer>) -> MutexGuard<'_, Sequencer> {
    // Every update leaves the sequencer consistent, so a panic elsewhere
    // cannot tear it.
    sequencer.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs an index job on the pool, reporting failures as the shard's.
pub(crate) async fn run<T: Send + 'static>(
    pool: &BlockingPool,
    shard: &ShardRef,
    job: impl FnOnce() -> Result<T, IndexError> + Send + 'static,
) -> Result<T, ShardError> {
    match pool.run(job).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ShardError::unavailable(shard, error)),
        Err(error) => Err(ShardError::unavailable(shard, error)),
    }
}

/// The task that applies a shard's records in position order and answers
/// their writers.
struct PipelineTask {
    shard: ShardRef,
    index: Arc<Index>,
    pool: BlockingPool,
    sequencer: Arc<Mutex<Sequencer>>,
    pipeline: Pipeline<WriteReply, BarrierReply>,
    /// Where the durable run of records is published.
    durable: watch::Sender<Seq>,
    /// A primary's replication state, told of every change.
    leader: Arc<OnceLock<Arc<Leader>>>,
    /// Why the shard stopped, once a record failed or applying did.
    stopped: Option<ShardError>,
}

impl PipelineTask {
    /// Runs until every handle to the shard, and every append in flight,
    /// is gone.
    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<Message>) {
        while let Some(message) = receiver.recv().await {
            self.accept(message);
            while let Ok(message) = receiver.try_recv() {
                self.accept(message);
            }
            self.publish_durable();
            while let Some(ready) = self.pipeline.next() {
                self.release(ready).await;
            }
            if let Some(leader) = self.leader.get() {
                leader.progress();
            }
        }
    }

    fn accept(&mut self, message: Message) {
        match message {
            Message::Sequenced { position, reply } => self.pipeline.sequenced(position, reply),
            Message::Appended { position, result } => {
                // A primary's CONFIG record of a configuration without some
                // members is durable: the commit rule and the leases stop
                // needing them (§6.4).
                if let (Some(leader), Ok(durable)) = (self.leader.get(), &result)
                    && let RecordBody::Config(config) = &durable.0.body
                {
                    leader.adopt(config);
                }
                self.pipeline.resolved(position, result);
            }
            Message::Marked { position, result } => {
                if let Err(error) = &result {
                    self.stop(format!("a TRUNCATE record did not become durable: {error}"));
                }
                self.pipeline.marker_resolved(position, result);
            }
            Message::Barrier(reply) => self.pipeline.barrier(reply),
            Message::Commit(seq) => self.pipeline.commit_through(seq),
            Message::Truncate { after, marker } => {
                self.pipeline.truncate(after);
                self.pipeline.marked(marker);
                self.durable.send_if_modified(|durable| {
                    let truncated = *durable > after;
                    *durable = (*durable).min(after);
                    truncated
                });
            }
            Message::Abandon(reply) => {
                let error = self.stop("the shard was closed".to_owned());
                let (writes, barriers) = self.pipeline.abandon();
                for write in writes {
                    let _ = write.send(Err(error.clone()));
                }
                for barrier in barriers {
                    let _ = barrier.send(Err(error.clone()));
                }
                let _ = reply.send(Ok(()));
            }
        }
    }

    /// Publishes the last `seq` of the durable run of records: the
    /// records the queue holds durably from its front on, after every
    /// record already applied.
    fn publish_durable(&self) {
        let applied = lock(&self.sequencer).applied.seq;
        let through = self
            .pipeline
            .durable_through()
            .map_or(applied, |position| position.seq.max(applied));
        self.durable.send_if_modified(|durable| {
            let advanced = through > *durable;
            if advanced {
                *durable = through;
            }
            advanced
        });
    }

    async fn release(&mut self, ready: Ready<WriteReply, BarrierReply>) {
        match ready {
            Ready::Barrier(reply) => {
                let _ = reply.send(self.stopped.clone().map_or(Ok(()), Err));
            }
            Ready::Failed(reply, error) => {
                let error = self.stop(format!("a record did not become durable: {error}"));
                let _ = reply.send(Err(error));
            }
            Ready::Apply(batch) => {
                if let Some(error) = &self.stopped {
                    for (_, reply) in batch {
                        let _ = reply.send(Err(error.clone()));
                    }
                    return;
                }
                let (records, replies): (Vec<_>, Vec<_>) = batch
                    .into_iter()
                    .map(|(durable, reply)| (*durable, reply))
                    .unzip();
                let positions: Vec<_> = records
                    .iter()
                    .map(|(record, _)| (record.position, is_client_write(&record.body)))
                    .collect();
                let mut changes: BTreeMap<EpochSeq, Change> =
                    records.iter().filter_map(|(r, _)| change(r)).collect();
                let index = Arc::clone(&self.index);
                let applied = run(&self.pool, &self.shard, move || {
                    let recorder = Recorder::default();
                    index.apply(&recorder, &records)?;
                    Ok(recorder.take())
                })
                .await;
                match applied {
                    Ok(outcomes) => {
                        // The subscriber is the one there is now, after the
                        // index holds the records: one that subscribed while
                        // they were applied may have scanned the index
                        // without them, so it must hear of them; one that
                        // subscribes later finds them in the index.
                        let (subscriber, under) = {
                            let mut sequencer = lock(&self.sequencer);
                            if let Some(&(last, _)) = positions.last() {
                                sequencer.applied = last;
                                sequencer.forget_applied();
                            }
                            let under = sequencer.under_replicated(&self.shard);
                            (sequencer.subscriber.clone(), under)
                        };
                        // Every record is past the applied position, so none
                        // was skipped and each has an outcome.
                        let mut outcomes: BTreeMap<_, _> = outcomes.into_iter().collect();
                        if let Some(subscriber) = subscriber {
                            report(&subscriber, &outcomes, &mut changes);
                        }
                        for ((position, client), reply) in positions.into_iter().zip(replies) {
                            let committed = outcomes
                                .remove(&position)
                                .map(|outcome| Committed { position, outcome })
                                .ok_or_else(|| {
                                    ShardError::unavailable(&self.shard, "a record was skipped")
                                });
                            // A client write that commits once too few
                            // members are left had fewer copies than
                            // min_write_replicas: applied, but not
                            // acknowledged (§5.2, §6.4).
                            let committed = match &under {
                                Some(error) if client => Err(error.clone()),
                                _ => committed,
                            };
                            let _ = reply.send(committed);
                        }
                    }
                    Err(error) => {
                        let error = self.stop(format!("applying failed: {error}"));
                        for reply in replies {
                            let _ = reply.send(Err(error.clone()));
                        }
                    }
                }
            }
        }
    }

    /// Stops the shard, if it is not stopped already, and returns the error
    /// every later request gets.
    fn stop(&mut self, reason: String) -> ShardError {
        self.stopped
            .get_or_insert_with(|| {
                let mut sequencer = lock(&self.sequencer);
                sequencer.stopped.get_or_insert_with(|| reason.clone());
                sequencer.subscriber = None;
                ShardError::unavailable(&self.shard, reason)
            })
            .clone()
    }
}

/// The task that queues a shard's records to its log in position order.
///
/// On a replicated shard it queues a record of the other segment class only
/// once every record queued before is durable: records of one class become
/// durable in the order they were queued, but the two classes are synced
/// independently, so a crash could otherwise keep a record without an
/// earlier one of the other class (§10.1). The replica's log therefore
/// always holds a run of the shard's records with no holes, which is what
/// its acknowledgements and catching up after a restart rely on. A
/// primary hands each record to its [`Leader`], to send to its members, as
/// soon as it is encoded.
struct Appender<D: Disk> {
    log: SegmentLog<D>,
    pipeline: mpsc::UnboundedSender<Message>,
    leader: Arc<OnceLock<Arc<Leader>>>,
    /// Whether records of different classes are queued in order.
    ordered: bool,
    /// The appends queued and not yet acknowledged.
    in_flight: JoinSet<()>,
    /// The class of the records in flight.
    class: SegmentClass,
}

impl<D: Disk> Appender<D> {
    /// Runs until every handle to the shard is gone, and the appends in
    /// flight then are acknowledged.
    async fn run(mut self, mut jobs: mpsc::UnboundedReceiver<Job>) {
        while let Some(job) = jobs.recv().await {
            while self.in_flight.try_join_next().is_some() {}
            match job {
                Job::Settle(reply) => {
                    self.drain().await;
                    let _ = reply.send(());
                }
                Job::Append {
                    record,
                    encoded,
                    lazy,
                } => self.append(*record, encoded, lazy).await,
                Job::Mark { record, reply } => self.mark(*record, reply).await,
            }
        }
        self.drain().await;
    }

    async fn drain(&mut self) {
        while self.in_flight.join_next().await.is_some() {}
    }

    async fn append(&mut self, record: LogRecord, encoded: Option<Bytes>, lazy: bool) {
        let position = record.position;
        let encoded = match encoded.map_or_else(|| record.to_bytes(), Ok) {
            Ok(encoded) => encoded,
            Err(error) => return self.resolved(position, Err(error.to_string())),
        };
        // Members append their CONFIG records for themselves.
        if let Some(leader) = self.leader.get()
            && !matches!(record.body, RecordBody::Config(_))
        {
            leader.push(position, encoded.clone(), lazy);
        }

        let class = SegmentClass::of(record.kind());
        if self.ordered && class != self.class {
            self.drain().await;
        }
        self.class = class;
        let queued = match self.log.queue_encoded(encoded, lazy).await {
            Ok(queued) => queued,
            Err(error) => return self.resolved(position, Err(error.to_string())),
        };
        let pipeline = self.pipeline.clone();
        self.in_flight.spawn(async move {
            let result = match queued.durable().await {
                Ok(location) => Ok(Box::new((record, location))),
                Err(error) => Err(error.to_string()),
            };
            // The pipeline is gone only if the shard is; nobody waits then.
            let _ = pipeline.send(Message::Appended { position, result });
        });
    }

    fn resolved(&self, position: EpochSeq, result: Result<Durable, String>) {
        let _ = self.pipeline.send(Message::Appended { position, result });
    }

    /// Appends a `TRUNCATE` record, which no writer waits on and nothing
    /// applies, and tells the pipeline and `reply` whether it became
    /// durable.
    async fn mark(&mut self, record: LogRecord, reply: oneshot::Sender<Result<(), String>>) {
        let (position, pipeline) = (record.position, self.pipeline.clone());
        let done = move |result: Result<(), String>| {
            let _ = pipeline.send(Message::Marked {
                position,
                result: result.clone(),
            });
            let _ = reply.send(result);
        };
        let encoded = match record.to_bytes() {
            Ok(encoded) => encoded,
            Err(error) => return done(Err(error.to_string())),
        };
        let class = SegmentClass::of(record.kind());
        if self.ordered && class != self.class {
            self.drain().await;
        }
        self.class = class;
        let queued = match self.log.queue_encoded(encoded, false).await {
            Ok(queued) => queued,
            Err(error) => return done(Err(error.to_string())),
        };
        self.in_flight.spawn(async move {
            done(queued.durable().await.map(drop).map_err(|e| e.to_string()));
        });
    }
}
