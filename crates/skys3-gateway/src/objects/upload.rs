//! Request bodies on their way into a shard: inline, or streamed as
//! `EXTENT` records while they arrive (§5.1).

use std::collections::VecDeque;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use s3s::{S3Error, S3Result, s3_error};
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef, Metadata, PutData, TagSet, UploadBegin};
use skys3_shard::StreamedBody;
use skys3_types::EpochSeq;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::buckets::shard_error;
use crate::conditions::Precondition;
use crate::shard::{ShardError, ShardRef, Shards};

/// How many `EXTENT` records of one body may be on their way to the shard
/// while the body is read on. Each holds up to `extent_bytes` in memory.
pub(crate) const MAX_EXTENTS_IN_FLIGHT: usize = 4;

/// A body being stored for a `PUT` of `key`.
///
/// Bytes collect in a buffer. Once the body is longer than
/// `inline_max_bytes`, every full `extent_bytes` of it is committed as an
/// `EXTENT` record, with at most [`MAX_EXTENTS_IN_FLIGHT`] in flight, and
/// [`Upload::finish`] commits the rest. A shorter body is returned whole, to
/// go inline in its `PUT`. The `PUT` itself is the caller's to commit, once
/// the body has passed its checks: extents of a body that fails are never
/// referenced, and compaction reclaims them (§10.3).
///
/// **Deadline.** Compaction drops extents that nothing names once their
/// segment has been sealed for `peer_staging_ttl_seconds`, which a body
/// still arriving cannot tell from garbage. A body therefore has
/// `max_duration`, half that TTL, from its first extent to finish: past it,
/// the upload fails with `400 RequestTimeout`, so every extent a committed
/// record names was written less than half the TTL before (§10.3). The
/// caller waits for each of the body's frames through
/// [`Upload::before_deadline`], so a client that stops sending is answered
/// then too, rather than holding the request open.
///
/// **Streamed PUTs.** An upload made [`Upload::streamed`] commits an
/// `UPLOAD_BEGIN` once its body reaches the threshold, and
/// [`Upload::identity`] then names it: the write identity its `PUT`
/// inherits (§7.2). The record is committed before any later byte is
/// taken, so the identity is durable before the body could stream to a
/// remote (§7.3). A body that fails leaves the record naming no write.
///
/// The upload then announces its body to the shard's primary
/// ([`Shards::announce`]): at once, with the metadata and tags the `PUT`
/// will store and the extents applied so far, and again each time another
/// `flush_part_bytes` of extents is applied, so that the primary's flusher
/// sends them to the remote while the body arrives. Announcements run on
/// their own and their failures are ignored, as the flusher sends what it
/// missed once the `PUT` commits; [`Upload::finish`] waits for them, so that
/// none reaches the flusher after the `PUT`.
pub(crate) struct Upload<H> {
    shards: H,
    shard: ShardRef,
    key: String,
    inline_max_bytes: usize,
    extent_bytes: usize,
    max_duration: Duration,
    buffer: BytesMut,
    /// The body offset of the buffer's first byte.
    offset: u64,
    streaming: bool,
    /// When the first extent was sent.
    started: Option<Instant>,
    in_flight: VecDeque<JoinHandle<Result<ExtentRef, ShardError>>>,
    extents: Vec<ExtentRef>,
    /// How the body streams to the remote, if it may.
    stream: Option<Streaming>,
    /// The position of the `UPLOAD_BEGIN`, once committed.
    begun: Option<EpochSeq>,
    /// How many of `extents` have been announced, and the body offset
    /// they end at.
    announced: (usize, u64),
    /// The announcements on their way to the primary.
    announcing: Vec<JoinHandle<()>>,
}

/// How a streamed PUT's body streams to its bucket's remote (§7.3).
#[derive(Debug, Clone)]
pub(crate) struct Streaming {
    /// `streaming_flush_min_bytes`: the body length at which to commit an
    /// `UPLOAD_BEGIN`.
    pub(crate) min_bytes: u64,
    /// `flush_part_bytes`: how many bytes of applied extents an
    /// announcement waits for.
    pub(crate) part_bytes: u64,
    /// The metadata the `PUT` will store.
    pub(crate) metadata: Metadata,
    /// The tags the `PUT` will store.
    pub(crate) tags: TagSet,
}

impl<H: Shards> Upload<H> {
    pub(crate) fn new(
        shards: H,
        shard: ShardRef,
        key: String,
        inline_max_bytes: usize,
        extent_bytes: usize,
        max_duration: Duration,
    ) -> Self {
        Self {
            shards,
            shard,
            key,
            inline_max_bytes,
            extent_bytes: extent_bytes.max(1),
            max_duration,
            buffer: BytesMut::new(),
            offset: 0,
            streaming: false,
            started: None,
            in_flight: VecDeque::new(),
            extents: Vec::new(),
            stream: None,
            begun: None,
            announced: (0, 0),
            announcing: Vec::new(),
        }
    }

    /// Makes the body a streamed PUT's once it reaches
    /// [`Streaming::min_bytes`], or never with `None` (see the type's docs).
    pub(crate) fn streamed(mut self, stream: Option<Streaming>) -> Self {
        self.stream = stream;
        self
    }

    /// The position of the `UPLOAD_BEGIN` the body committed, whose write
    /// identity its `PUT` inherits, or `None` if it committed none.
    pub(crate) fn identity(&self) -> Option<EpochSeq> {
        self.begun
    }

    /// Waits for `next`, the body's next frame, for no longer than the body
    /// has left once its first extent is sent (see the type's docs).
    ///
    /// # Errors
    ///
    /// `400 RequestTimeout` if the deadline passes first; `next` is then
    /// dropped.
    pub(crate) async fn before_deadline<F: Future>(&self, next: F) -> S3Result<F::Output> {
        let deadline = self
            .started
            .and_then(|started| started.checked_add(self.max_duration));
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, next)
                .await
                .map_err(|_| self.timed_out()),
            None => Ok(next.await),
        }
    }

    /// Takes the body's next bytes, committing every extent they fill, and
    /// the `UPLOAD_BEGIN` of a streamed PUT once they reach its threshold.
    pub(crate) async fn push(&mut self, data: &[u8]) -> S3Result<()> {
        self.buffer.extend_from_slice(data);
        let received = self.offset + self.buffer.len() as u64;
        let threshold = self.stream.as_ref().map(|stream| stream.min_bytes);
        if self.begun.is_none() && threshold.is_some_and(|min| received >= min) {
            self.begin().await?;
            self.announce(true);
        }
        self.streaming |= self.buffer.len() > self.inline_max_bytes;
        while self.streaming && self.buffer.len() >= self.extent_bytes {
            let extent = self.buffer.split_to(self.extent_bytes).freeze();
            self.send(extent).await?;
        }
        Ok(())
    }

    /// Commits the `UPLOAD_BEGIN` that fixes the body's write identity.
    async fn begin(&mut self) -> S3Result<()> {
        let begin = RecordBody::UploadBegin(UploadBegin {
            key: self.key.clone(),
        });
        let written = self
            .shards
            .write(&self.shard, begin, Precondition::None)
            .await
            .map_err(shard_error)?;
        match written {
            Ok(position) => {
                self.begun = Some(position);
                Ok(())
            }
            // A record without a condition never fails one.
            Err(failed) => {
                tracing::error!(shard = %self.shard, ?failed, "an UPLOAD_BEGIN failed a condition");
                Err(s3_error!(InternalError))
            }
        }
    }

    /// Announces the extents applied since the last announcement to the
    /// primary of a streamed body, if there are `flush_part_bytes` of them
    /// or `now` (see the type's docs).
    fn announce(&mut self, now: bool) {
        let (Some(stream), Some(upload)) = (&self.stream, self.begun) else {
            return;
        };
        let (count, mut offset) = self.announced;
        let fresh = &self.extents[count..];
        let bytes: u64 = fresh.iter().map(|extent| u64::from(extent.len)).sum();
        if !now && bytes < stream.part_bytes {
            return;
        }
        let extents = fresh
            .iter()
            .map(|extent| {
                let at = offset;
                offset += u64::from(extent.len);
                (at, *extent)
            })
            .collect();
        self.announced = (self.extents.len(), offset);
        let body = StreamedBody {
            key: self.key.clone(),
            upload,
            metadata: stream.metadata.clone(),
            tags: stream.tags.clone(),
            extents,
        };
        let (shards, shard) = (self.shards.clone(), self.shard.clone());
        self.announcing.retain(|task| !task.is_finished());
        self.announcing.push(tokio::spawn(async move {
            if let Err(error) = shards.announce(&shard, body).await {
                tracing::debug!(%shard, %error, "a streamed body was not announced");
            }
        }));
    }

    /// Commits what is left of the body, waits until every extent is
    /// applied, and returns where the body's bytes are.
    pub(crate) async fn finish(mut self) -> S3Result<PutData> {
        if !self.streaming {
            self.announcements().await;
            return Ok(PutData::Inline(self.buffer.freeze()));
        }
        if !self.buffer.is_empty() {
            let rest = self.buffer.split().freeze();
            self.send(rest).await?;
        }
        while let Some(sent) = self.in_flight.pop_front() {
            self.extents.push(joined(sent).await?);
        }
        self.announcements().await;
        self.check_deadline()?;
        Ok(PutData::Extents(self.extents))
    }

    /// Waits for the announcements on their way.
    async fn announcements(&mut self) {
        for task in std::mem::take(&mut self.announcing) {
            let _ = task.await;
        }
    }

    /// Commits one extent, waiting first for the oldest if too many are in
    /// flight. The append continues if the upload is dropped.
    async fn send(&mut self, data: Bytes) -> S3Result<()> {
        self.check_deadline()?;
        if self.in_flight.len() >= MAX_EXTENTS_IN_FLIGHT
            && let Some(oldest) = self.in_flight.pop_front()
        {
            self.extents.push(joined(oldest).await?);
            self.announce(false);
        }
        let extent = Extent {
            key: self.key.clone(),
            offset: self.offset,
            data,
        };
        self.offset += extent.data.len() as u64;
        self.started.get_or_insert_with(Instant::now);
        let (shards, shard) = (self.shards.clone(), self.shard.clone());
        self.in_flight.push_back(tokio::spawn(async move {
            shards.append_extent(&shard, extent).await
        }));
        Ok(())
    }

    /// Fails the body once it has streamed for longer than it may (see the
    /// type's docs).
    fn check_deadline(&self) -> S3Result<()> {
        match self.started {
            Some(started) if started.elapsed() > self.max_duration => Err(self.timed_out()),
            _ => Ok(()),
        }
    }

    fn timed_out(&self) -> S3Error {
        s3_error!(
            RequestTimeout,
            "The body took longer than {:?} to arrive",
            self.max_duration
        )
    }
}

async fn joined(sent: JoinHandle<Result<ExtentRef, ShardError>>) -> S3Result<ExtentRef> {
    match sent.await {
        Ok(appended) => appended.map_err(shard_error),
        Err(error) => {
            tracing::error!(%error, "an extent append panicked");
            Err(s3_error!(InternalError))
        }
    }
}
