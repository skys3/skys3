//! What a replica does for backfill (§6.4, §6.7): a primary reads a
//! snapshot of its shard and the records that hold payload, and a learner
//! installs the snapshot in place of its log and stores the payload it
//! lacks.
//!
//! - **The snapshot** is the shard's rows in the index, read in one read
//!   transaction at the primary's applied position. A clean entry of a
//!   `write_back` bucket goes as evicted: its payload is not copied, since
//!   the remote holds it (§6.7). A clean entry of a bucket whose payload
//!   the node keeps ([`Shard::evicts_clean`]) goes as it is: a `local`
//!   bucket's replicas are its durable home, also once its backup target
//!   holds the entry (§8.9).
//! - **Installing** discards what the learner held: a `TRUNCATE` at
//!   `(epoch, 0)` invalidates every record of an older epoch, and the
//!   index marks the shard as installing at `(epoch, Seq::MAX)`, after
//!   every record the log can hold, so replay applies none of them even
//!   if the install is cut short; a replica that opens with the marker
//!   holds nothing (see [`Shard::open_replica`]). The last step sets the
//!   applied position to the snapshot's, durably.
//! - **Payload** is named by log position (§10.2). A learner finds the
//!   positions that its entries need and that it does not locate: those
//!   of every entry that is not clean (dirty, flushing, or in conflict),
//!   of every clean entry of a bucket whose payload the node keeps (in a
//!   `local` bucket, every entry), and those of every open multipart
//!   upload. It stores the records the primary sends for them
//!   in its own log, unapplied, as they are at or before its applied
//!   position, and records their locations durably.

use std::sync::Arc;

use bytes::Bytes;
use skys3_index::{EntryState, IndexError, IndexReader, Payload, ShardRow, ShardTable, codec};
use skys3_io::Disk;
use skys3_log::record::{Extent, MpuPart, Put, PutData};
use skys3_log::{LogRecord, RecordBody};
use skys3_types::{Epoch, EpochSeq, Seq};

use tokio::sync::oneshot;

use super::{Message, Shard, run};
use crate::error::ShardError;

/// Where a scan for missing payload resumes: in the namespace after a key,
/// or in the open uploads after one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FillCursor {
    /// The entries after this key, from the first if `None`.
    Entries(Option<String>),
    /// The open uploads after this key and upload, from the first if
    /// `None`.
    Uploads(Option<(String, EpochSeq)>),
}

impl Default for FillCursor {
    fn default() -> Self {
        Self::Entries(None)
    }
}

/// How many entries, or open uploads, one scan for missing payload reads.
const FILL_PAGE: usize = 256;

/// How many bytes of rows one snapshot chunk carries, at most, unless one
/// row is larger.
pub(crate) const CHUNK_BYTES: usize = 1 << 20;

/// The payload bytes of `record`, if it holds payload: an `EXTENT`, or a
/// `PUT` or `MPU_PART` with inline data.
pub(super) fn payload_of(record: &LogRecord) -> Option<&Bytes> {
    match &record.body {
        RecordBody::Extent(Extent { data, .. })
        | RecordBody::Put(Put {
            data: PutData::Inline(data),
            ..
        })
        | RecordBody::MpuPart(MpuPart {
            data: PutData::Inline(data),
            ..
        }) => Some(data),
        _ => None,
    }
}

/// The positions of the records that hold `payload`'s bytes, except the
/// parts of a multipart object, which `parts` lists.
fn positions(payload: &Payload, into: &mut Vec<EpochSeq>) {
    match payload {
        Payload::Inline(position) => into.push(*position),
        Payload::Extents(extents) => into.extend(extents.iter().map(|extent| extent.position)),
        Payload::None | Payload::Parts { .. } => {}
    }
}

impl<D: Disk> Shard<D> {
    /// Stops the replica at once: what it queued fails, and nothing it
    /// holds past its applied position is applied. A learner does so
    /// before it installs a snapshot in place of everything it holds.
    pub(crate) async fn abandon(&self, reason: &str) {
        {
            let mut sequencer = self.sequencer();
            sequencer.stopped.get_or_insert_with(|| reason.to_owned());
            sequencer.subscriber = None;
        }
        let (reply, abandoned) = oneshot::channel();
        if self.inner.pipeline.send(Message::Abandon(reply)).is_ok() {
            // The pipeline answers before it ends, and ends with the shard.
            let _ = abandoned.await;
        }
        self.settle().await;
    }

    /// The epoch of the record at `seq`, if this replica holds it durably
    /// and knows it: from its lineage, or from its log. A primary checks a
    /// re-admitted learner's last record against its own this way, and the
    /// learner reports it so (§6.7).
    pub(crate) async fn record_epoch(&self, seq: Seq) -> Option<Epoch> {
        if seq == Seq::ZERO || seq > *self.inner.durable.borrow() {
            return None;
        }
        if let Some(epoch) = self.sequencer().lineage.epoch_at(seq) {
            return Some(epoch);
        }
        let before = Seq::new(seq.get() - 1);
        let records = self.read_tail(before, seq).await.ok()?;
        records.first().map(|(position, _)| position.epoch)
    }

    /// A consistent view of the index for a snapshot, and the applied
    /// position the snapshot is taken at.
    pub(crate) async fn snapshot(&self) -> Result<(Arc<IndexReader>, EpochSeq), ShardError> {
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            let reader = index.read()?;
            let applied = reader.applied(&shard)?.unwrap_or_default();
            Ok((Arc::new(reader), applied))
        })
        .await
    }

    /// The next chunk of the snapshot's rows in `table` after the key
    /// `after`, as a learner gets them: clean entries evicted, if this
    /// node evicts the bucket's clean payload ([`Shard::evicts_clean`]).
    /// A `local` bucket's clean entries, which a backup target made clean,
    /// keep their payload (§8.9).
    pub(crate) async fn snapshot_rows(
        &self,
        reader: &Arc<IndexReader>,
        table: ShardTable,
        after: Option<Vec<u8>>,
    ) -> Result<Vec<ShardRow>, ShardError> {
        let (reader, shard) = (Arc::clone(reader), self.shard().clone());
        let evicts = self.evicts_clean();
        run(&self.inner.pool, self.shard(), move || {
            let mut rows = reader.shard_rows(table, &shard, after.as_deref(), CHUNK_BYTES)?;
            if table == ShardTable::Namespace && evicts {
                let codec = |source| IndexError::Codec {
                    table: "namespace",
                    source,
                };
                for (_, value) in &mut rows {
                    let mut entry = codec::decode_entry(value).map_err(codec)?;
                    if entry.state == EntryState::Clean {
                        entry.state = EntryState::Evicted;
                        if let Some(object) = &mut entry.object {
                            object.payload = Payload::None;
                        }
                        *value = codec::encode_entry(&entry).map_err(codec)?;
                    }
                }
            }
            Ok(rows)
        })
        .await
    }

    /// Starts installing a snapshot of a configuration in `epoch` in place
    /// of everything this replica holds, which must have stopped: see the
    /// [module](self) docs.
    pub(crate) async fn begin_install(&self, epoch: Epoch) -> Result<(), ShardError> {
        let shard = self.shard();
        if !self.is_stopped() {
            return Err(self.unavailable("the replica installs a snapshot only once stopped"));
        }
        let truncate = LogRecord {
            shard: shard.clone(),
            position: EpochSeq::new(epoch, Seq::ZERO),
            body: RecordBody::Truncate,
        };
        self.inner
            .log
            .append(&truncate)
            .await
            .map_err(|error| ShardError::unavailable(shard, error))?;
        let (index, key) = (Arc::clone(&self.inner.index), shard.clone());
        let marker = EpochSeq::new(epoch, Seq::MAX);
        run(&self.inner.pool, shard, move || {
            index.begin_install(&key, marker)
        })
        .await
    }

    /// Stores rows of a snapshot being installed.
    pub(crate) async fn install_rows(
        &self,
        table: ShardTable,
        rows: Vec<ShardRow>,
    ) -> Result<(), ShardError> {
        let (index, key) = (Arc::clone(&self.inner.index), self.shard().clone());
        run(&self.inner.pool, self.shard(), move || {
            index.install_rows(&key, table, &rows)
        })
        .await
    }

    /// Finishes installing a snapshot taken at `at`, in the replica's
    /// configuration, which the index keeps as the shard's configuration
    /// from then on (§6.2).
    pub(crate) async fn finish_install(&self, at: EpochSeq) -> Result<(), ShardError> {
        let (index, config) = (Arc::clone(&self.inner.index), self.config());
        run(&self.inner.pool, self.shard(), move || {
            index.finish_install(&config, at)
        })
        .await
    }

    /// Up to a page of the positions whose payload this replica's entries,
    /// from `cursor` on, need and it does not locate, and where the next
    /// page starts, or `None` after the last. Clean entries need none if
    /// this node evicts the bucket's clean payload
    /// ([`Shard::evicts_clean`]).
    pub(crate) async fn missing_payload(
        &self,
        cursor: FillCursor,
    ) -> Result<(Vec<EpochSeq>, Option<FillCursor>), ShardError> {
        let (index, shard) = (Arc::clone(&self.inner.index), self.shard().clone());
        let evicts = self.evicts_clean();
        run(&self.inner.pool, self.shard(), move || {
            let reader = index.read()?;
            let mut needed = Vec::new();
            let mut uploads = Vec::new();
            let next = match cursor {
                FillCursor::Entries(after) => {
                    let page = reader.entries(&shard, after.as_deref(), FILL_PAGE)?;
                    let full = page.len() == FILL_PAGE;
                    for (_, entry) in &page {
                        let Some(object) = &entry.object else {
                            continue;
                        };
                        let evictable = match entry.state {
                            EntryState::Evicted => true,
                            EntryState::Clean => evicts,
                            _ => false,
                        };
                        if evictable {
                            continue;
                        }
                        positions(&object.payload, &mut needed);
                        if let Payload::Parts { upload, .. } = &object.payload {
                            uploads.push(*upload);
                        }
                    }
                    match page.into_iter().last() {
                        Some((key, _)) if full => Some(FillCursor::Entries(Some(key))),
                        _ => Some(FillCursor::Uploads(None)),
                    }
                }
                FillCursor::Uploads(after) => {
                    let after = after.as_ref().map(|(key, at)| (key.as_str(), Some(*at)));
                    let page = reader.uploads(&shard, "", after, FILL_PAGE)?;
                    uploads.extend(page.iter().map(|(_, upload, _)| *upload));
                    page.into_iter()
                        .last()
                        .map(|(key, upload, _)| FillCursor::Uploads(Some((key, upload))))
                }
            };
            for upload in uploads {
                for (_, part) in reader.parts(&shard, upload, 0, usize::MAX)? {
                    positions(&part.payload, &mut needed);
                }
            }
            needed.sort_unstable();
            needed.dedup();
            let mut missing = Vec::new();
            for position in needed {
                if reader.location(&shard, position)?.is_none() {
                    missing.push(position);
                }
            }
            Ok((missing, next))
        })
        .await
    }

    /// The encoded record at `position` that holds payload, as a learner
    /// stores it, or `None` if this replica locates no such record.
    pub(crate) async fn payload_record(
        &self,
        position: EpochSeq,
    ) -> Result<Option<Bytes>, ShardError> {
        let shard = self.shard();
        let (index, key) = (Arc::clone(&self.inner.index), shard.clone());
        let location = run(&self.inner.pool, shard, move || {
            index.read()?.location(&key, position)
        })
        .await?;
        let Some(location) = location else {
            return Ok(None);
        };
        let failed = |error: &dyn std::fmt::Display| ShardError::unavailable(shard, error);
        let record = self
            .inner
            .log
            .read(location)
            .await
            .map_err(|e| failed(&e))?;
        if record.shard != *shard || record.position != position || payload_of(&record).is_none() {
            return Ok(None);
        }
        record.to_bytes().map(Some).map_err(|e| failed(&e))
    }

    /// Stores `records`, each an encoded record holding payload at the
    /// position it names, in this replica's log, and records their
    /// locations durably. They are history: at or before the applied
    /// position, so nothing applies them.
    ///
    /// # Errors
    ///
    /// [`ShardError::InvalidRecord`] if a record does not decode, is of
    /// another shard or position, holds no payload, or is past the applied
    /// position; [`ShardError::Unavailable`] if the log or the index fails.
    pub(crate) async fn store_payload(
        &self,
        records: Vec<(EpochSeq, Bytes)>,
    ) -> Result<(), ShardError> {
        let shard = self.shard();
        let applied = self.applied();
        let mut queued = Vec::with_capacity(records.len());
        for (position, bytes) in records {
            let record = match LogRecord::decode(&bytes) {
                Ok((record, len)) if len == bytes.len() => record,
                Ok(_) => {
                    return Err(ShardError::invalid(
                        shard,
                        "a copied record has trailing bytes",
                    ));
                }
                Err(error) => return Err(ShardError::invalid(shard, error.to_string())),
            };
            if record.shard != *shard
                || record.position != position
                || position > applied
                || payload_of(&record).is_none()
            {
                return Err(ShardError::invalid(
                    shard,
                    format!("the record copied for {position} is not its payload"),
                ));
            }
            let pending = self.inner.log.queue_encoded(bytes, false).await;
            queued.push((
                position,
                pending.map_err(|e| ShardError::unavailable(shard, e))?,
            ));
        }
        let mut locations = Vec::with_capacity(queued.len());
        for (position, pending) in queued {
            let location = pending
                .durable()
                .await
                .map_err(|error| ShardError::unavailable(shard, error))?;
            locations.push((position, location));
        }
        let (index, key) = (Arc::clone(&self.inner.index), shard.clone());
        run(&self.inner.pool, shard, move || {
            index.store_locations(&key, &locations)
        })
        .await
    }
}
