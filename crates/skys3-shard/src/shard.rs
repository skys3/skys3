//! A shard replica on one node: sequencing, the commit pipeline, and seals
//! (§5.1, §4.1).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_index::{Entry, EntryState, Index, IndexError, Part, Upload};
use skys3_io::{BlockingPool, Disk};
use skys3_log::record::{Extent, ExtentRef, MpuPart, Put, PutData};
use skys3_log::{LogRecord, RecordBody, SegmentLog, ShardRef};
use skys3_types::{Epoch, EpochSeq, Seq, ShardConfig};
use tokio::sync::{mpsc, oneshot};

use crate::error::ShardError;
use crate::machine::{Outcome, Recorder, StateMachine};
use crate::pipeline::{Durable, Pipeline, Ready};

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

type WriteReply = oneshot::Sender<Result<Committed, ShardError>>;
type BarrierReply = oneshot::Sender<Result<(), ShardError>>;

/// What the shard's pipeline task receives.
enum Message {
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
}

/// The sequencing state, changed only under its lock.
#[derive(Debug)]
struct Sequencer {
    /// The position the next record gets.
    next: EpochSeq,
    /// The last position applied to the index.
    applied: EpochSeq,
    /// How many seals are held (§4.1).
    seals: u32,
    /// The configuration the shard is in: `next` is in its epoch.
    config: ShardConfig,
    /// Why the shard stopped, once it has.
    stopped: Option<String>,
    /// The position of the latest record sequenced for each key, other than
    /// an `EXTENT`, while it is not applied, and while a conditional read
    /// that began at or before it is in progress (see
    /// [`Shard::commit_if`]).
    writes: BTreeMap<String, EpochSeq>,
    /// Conditional reads in progress, counted by the position the next
    /// record had when each began.
    readers: BTreeMap<EpochSeq, usize>,
}

struct Inner<D: Disk> {
    shard: ShardRef,
    log: SegmentLog<D>,
    index: Arc<Index>,
    pool: BlockingPool,
    sequencer: Arc<Mutex<Sequencer>>,
    pipeline: mpsc::UnboundedSender<Message>,
}

/// One shard replica on this node, with `replicas = 1`: the shard's only
/// member, and so its primary (§5.1).
///
/// - **Sequencing.** Each record gets the next position `(epoch, seq)` of
///   the shard's log. It is checked before it gets one, so a record the
///   log would refuse never leaves a gap.
/// - **Commit.** With one member, a record commits once the log has made
///   it durable. Records become durable out of order, so a pipeline applies
///   them to the index in position order: a record is applied, and its
///   writer answered, only once every earlier record is durable and
///   applied. An `EXTENT` is applied like any record, so a `PUT` that
///   references extents is accepted only once they are applied.
/// - **Failure.** A record that cannot be made durable stops the shard: it
///   and every later record fail, and the shard refuses writes until the
///   node reopens it after replaying its log. Its disk is out of service by
///   then anyway (§10.4).
/// - **Seals** ([`Shard::seal`], [`Shard::unseal`]) are ordered with the
///   shard's writes, as deleting a bucket requires (§4.1).
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
    /// [`ShardError::Configuration`] if `config` has another member than
    /// this one or any learner (replication arrives in M2), or is older than
    /// an epoch the shard has applied; [`ShardError::Unavailable`] if the
    /// index or the log fails.
    pub async fn open(
        config: &ShardConfig,
        log: SegmentLog<D>,
        index: Arc<Index>,
        pool: BlockingPool,
    ) -> Result<Self, ShardError> {
        let shard = ShardRef::new(config.bucket_id.clone(), config.shard);
        check_config(&shard, config)?;
        let applied = {
            let (index, key) = (Arc::clone(&index), shard.clone());
            run(&pool, &shard, move || index.read()?.applied(&key)).await?
        };
        let epoch = config.epoch;
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
        if applied.is_none_or(|applied| applied.epoch < epoch) {
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
            next: EpochSeq::new(epoch, next),
            applied: last,
            seals: 0,
            config: config.clone(),
            stopped: None,
            writes: BTreeMap::new(),
            readers: BTreeMap::new(),
        }));
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = PipelineTask {
            shard: shard.clone(),
            index: Arc::clone(&index),
            pool: pool.clone(),
            sequencer: Arc::clone(&sequencer),
            pipeline: Pipeline::default(),
            stopped: None,
        };
        tokio::spawn(task.run(receiver));
        Ok(Self {
            inner: Arc::new(Inner {
                shard,
                log,
                index,
                pool,
                sequencer,
                pipeline: sender,
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

    /// The configuration the shard is in.
    #[must_use]
    pub fn config(&self) -> ShardConfig {
        self.sequencer().config.clone()
    }

    /// Whether any seal is held.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sequencer().seals > 0
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
    /// The record is appended even if the returned future is dropped.
    ///
    /// # Errors
    ///
    /// [`ShardError::Sealed`]; [`ShardError::InvalidRecord`] if the record
    /// cannot be encoded, references an extent that is not applied, or is a
    /// `CONFIG` or `TRUNCATE`, which a replica appends for itself; and
    /// [`ShardError::Unavailable`] if the shard stopped.
    pub async fn commit(&self, body: RecordBody) -> Result<Committed, ShardError> {
        let reply = self.submit(body)?;
        reply
            .await
            .unwrap_or_else(|_| Err(self.unavailable("the shard's pipeline stopped")))
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

    /// Sequences `body` and starts its append, under the sequencer's lock, so
    /// records reach the pipeline in position order.
    fn submit(
        &self,
        body: RecordBody,
    ) -> Result<oneshot::Receiver<Result<Committed, ShardError>>, ShardError> {
        self.submit_locked(&mut self.sequencer(), body)
    }

    /// [`Shard::submit`], for a caller that holds the sequencer's lock.
    fn submit_locked(
        &self,
        sequencer: &mut Sequencer,
        body: RecordBody,
    ) -> Result<oneshot::Receiver<Result<Committed, ShardError>>, ShardError> {
        let shard = self.shard();
        sequencer.check_running(shard)?;
        let client_write = match &body {
            RecordBody::Put(_) | RecordBody::Delete(_) | RecordBody::Tags(_) => true,
            RecordBody::Extent(_) => true,
            RecordBody::MpuCreate(_)
            | RecordBody::MpuPart(_)
            | RecordBody::MpuComplete(_)
            | RecordBody::MpuAbort(_) => true,
            RecordBody::Config(_) | RecordBody::Truncate => {
                return Err(ShardError::invalid(
                    shard,
                    "CONFIG and TRUNCATE records are appended by the replica itself",
                ));
            }
            _ => false,
        };
        if client_write && sequencer.seals > 0 {
            return Err(ShardError::Sealed(shard.clone()));
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
        let key = entry_key(&body).map(str::to_owned);
        let receiver = self.sequence(position, body)?;
        sequencer.next = EpochSeq::new(position.epoch, next);
        if let Some(key) = key {
            sequencer.writes.insert(key, position);
        }
        Ok(receiver)
    }

    /// Commits `body`, a record that names a key, if `check` accepts the
    /// key's entry as it is when the record is sequenced: the outcome of
    /// every record of the key sequenced before it, applied or not. If
    /// `check` refuses, nothing is appended and its error is returned.
    ///
    /// The check and the record's position are therefore linearizable with
    /// every other write of the key, as conditional requests need (§5.1).
    /// While a record of the key is sequenced but not yet applied, the check
    /// waits until it is. `check` may run more than once, and runs under the
    /// sequencer's lock, so it must be quick.
    ///
    /// # Errors
    ///
    /// The outer error as [`Shard::commit`], or
    /// [`ShardError::InvalidRecord`] for a record that names no key or is an
    /// `EXTENT`; the inner one is `check`'s.
    pub async fn commit_if<E>(
        &self,
        body: RecordBody,
        mut check: impl FnMut(Option<&Entry>) -> Result<(), E>,
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
                sequencer.check_running(self.shard())?;
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
                self.submit_locked(&mut sequencer, body)?
            };
            return reply
                .await
                .unwrap_or_else(|_| Err(self.unavailable("the shard's pipeline stopped")))
                .map(Ok);
        }
    }

    /// The entry of `key` as the index holds it: the outcome of every
    /// applied record of the key, and of no record not yet applied.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn entry(&self, key: &str) -> Result<Option<Entry>, ShardError> {
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

    /// The open upload of `key` opened at `upload`, as the index holds it.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn upload(&self, key: &str, upload: EpochSeq) -> Result<Option<Upload>, ShardError> {
        let (index, shard, key) = (
            Arc::clone(&self.inner.index),
            self.shard().clone(),
            key.to_owned(),
        );
        run(&self.inner.pool, self.shard(), move || {
            index.read()?.upload(&shard, &key, upload)
        })
        .await
    }

    /// Up to `limit` open uploads whose keys start with `prefix`, by key and
    /// age, after `after` (see
    /// [`IndexReader::uploads`](skys3_index::IndexReader::uploads)).
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn uploads(
        &self,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
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
    /// [`ShardError::Unavailable`] if the index fails.
    pub async fn parts(
        &self,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
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
        match record.body {
            RecordBody::Extent(Extent { data, .. })
            | RecordBody::Put(Put {
                data: PutData::Inline(data),
                ..
            })
            | RecordBody::MpuPart(MpuPart {
                data: PutData::Inline(data),
                ..
            }) if record.shard == *shard && record.position == position => Ok(data),
            _ => Err(self.unavailable(&format!("the record at {position} holds no payload"))),
        }
    }

    /// Adopts `config`, a newer epoch of the shard, in order with its
    /// writes, and returns once its `CONFIG` record is durable and applied.
    ///
    /// The `CONFIG` record takes the position `(epoch, last seq)`, after
    /// every record sequenced so far, and every later record is sequenced in
    /// the new epoch (§10.1). As every record, it is applied, and the writers
    /// of the records after it answered, only once it is durable, so nothing
    /// is acknowledged in the new epoch before the configuration is. The
    /// configuration the shard is in already changes nothing, even on a
    /// stopped shard.
    ///
    /// # Errors
    ///
    /// [`ShardError::Configuration`] if `config` is of another shard, is
    /// older than the shard's epoch, differs from the shard's configuration
    /// in the same epoch, or cannot open (see [`Shard::open`]);
    /// [`ShardError::Unavailable`] if the shard stopped, and otherwise as
    /// [`Shard::commit`].
    pub async fn reconfigure(&self, config: &ShardConfig) -> Result<(), ShardError> {
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
            let epoch = sequencer.next.epoch;
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
                Ordering::Equal if *config == sequencer.config => return Ok(()),
                Ordering::Equal => {
                    return Err(ShardError::configuration(
                        shard,
                        format!("the configuration differs from the shard's in epoch {epoch}"),
                    ));
                }
                Ordering::Greater => {}
            }
            sequencer.check_running(shard)?;
            check_config(shard, config)?;
            // `next.seq` is at least 1: opening starts it after the CONFIG
            // record at `seq` 0 or later.
            let last = Seq::new(sequencer.next.seq.get() - 1);
            let position = EpochSeq::new(config.epoch, last);
            let reply = self.sequence(position, RecordBody::Config(config.clone()))?;
            sequencer.next = EpochSeq::new(config.epoch, sequencer.next.seq);
            sequencer.config = config.clone();
            reply
        };
        reply
            .await
            .unwrap_or_else(|_| Err(self.unavailable("the shard's pipeline stopped")))
            .map(drop)
    }

    /// Gives `body` the position `position` and starts its append; the
    /// caller holds the sequencer's lock and advances `next` on success.
    fn sequence(
        &self,
        position: EpochSeq,
        body: RecordBody,
    ) -> Result<oneshot::Receiver<Result<Committed, ShardError>>, ShardError> {
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
        self.inner
            .pipeline
            .send(Message::Sequenced { position, reply })
            .map_err(|_| self.unavailable("the shard's pipeline stopped"))?;
        let (log, pipeline) = (self.inner.log.clone(), self.inner.pipeline.clone());
        tokio::spawn(async move {
            let result = match log.append(&record).await {
                Ok(location) => Ok(Box::new((record, location))),
                Err(error) => Err(error.to_string()),
            };
            // The pipeline is gone only if the shard is; nobody waits then.
            let _ = pipeline.send(Message::Appended { position, result });
        });
        Ok(receiver)
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
    /// [`ShardError::Unavailable`] if the shard stopped or its index fails.
    pub async fn seal(&self) -> Result<ShardSummary, ShardError> {
        let barrier = {
            let mut sequencer = self.sequencer();
            sequencer.check_running(self.shard())?;
            sequencer.seals += 1;
            self.barrier()
        };
        let mut guard = SealGuard(Some(self));
        barrier.await?;
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
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard had stopped already.
    pub async fn close(&self) -> Result<(), ShardError> {
        let barrier = {
            let mut sequencer = self.sequencer();
            sequencer.check_running(self.shard())?;
            sequencer.stopped = Some("the shard was closed".to_owned());
            self.barrier()
        };
        barrier.await
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

    /// Counts the shard's entries.
    async fn summary(&self) -> Result<ShardSummary, ShardError> {
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            let reader = index.read()?;
            let mut summary = ShardSummary::default();
            let mut after = None;
            loop {
                let page = reader.entries(&shard, after.as_deref(), SUMMARY_PAGE)?;
                for (_, entry) in &page {
                    summary.objects += u64::from(entry.object.is_some());
                    summary.unflushed += u64::from(!matches!(
                        entry.state,
                        EntryState::Clean | EntryState::Evicted
                    ));
                }
                match page.into_iter().last() {
                    Some((key, _)) => after = Some(key),
                    None => return Ok(summary),
                }
            }
        })
        .await
    }

    fn sequencer(&self) -> MutexGuard<'_, Sequencer> {
        lock(&self.inner.sequencer)
    }

    fn unavailable(&self, reason: &str) -> ShardError {
        ShardError::unavailable(self.shard(), reason)
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

/// Checks that this build can serve `config`: a single member without
/// learners, until replication arrives in M2.
fn check_config(shard: &ShardRef, config: &ShardConfig) -> Result<(), ShardError> {
    if config.members.len() != 1 || !config.learners.is_empty() {
        return Err(ShardError::configuration(
            shard,
            "only a single member without learners is supported",
        ));
    }
    Ok(())
}

fn lock(sequencer: &Mutex<Sequencer>) -> MutexGuard<'_, Sequencer> {
    // Every update leaves the sequencer consistent, so a panic elsewhere
    // cannot tear it.
    sequencer.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs an index job on the pool, reporting failures as the shard's.
async fn run<T: Send + 'static>(
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
            while let Some(ready) = self.pipeline.next() {
                self.release(ready).await;
            }
        }
    }

    fn accept(&mut self, message: Message) {
        match message {
            Message::Sequenced { position, reply } => self.pipeline.sequenced(position, reply),
            Message::Appended { position, result } => self.pipeline.resolved(position, result),
            Message::Barrier(reply) => self.pipeline.barrier(reply),
        }
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
                let positions: Vec<_> = records.iter().map(|(record, _)| record.position).collect();
                let index = Arc::clone(&self.index);
                let applied = run(&self.pool, &self.shard, move || {
                    let recorder = Recorder::default();
                    index.apply(&recorder, &records)?;
                    Ok(recorder.take())
                })
                .await;
                match applied {
                    Ok(outcomes) => {
                        if let Some(&last) = positions.last() {
                            let mut sequencer = lock(&self.sequencer);
                            sequencer.applied = last;
                            sequencer.forget_applied();
                        }
                        // Every record is past the applied position, so none
                        // was skipped and each has an outcome.
                        let mut outcomes: BTreeMap<_, _> = outcomes.into_iter().collect();
                        for (position, reply) in positions.into_iter().zip(replies) {
                            let committed = outcomes
                                .remove(&position)
                                .map(|outcome| Committed { position, outcome })
                                .ok_or_else(|| {
                                    ShardError::unavailable(&self.shard, "a record was skipped")
                                });
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
                ShardError::unavailable(&self.shard, reason)
            })
            .clone()
    }
}
