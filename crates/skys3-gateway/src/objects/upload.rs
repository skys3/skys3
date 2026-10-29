//! Request bodies on their way into a shard: inline, or streamed as
//! `EXTENT` records while they arrive (§5.1).

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use s3s::{S3Result, s3_error};
use skys3_log::record::{Extent, ExtentRef, PutData};
use tokio::task::JoinHandle;

use crate::buckets::shard_error;
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
pub(crate) struct Upload<H> {
    shards: H,
    shard: ShardRef,
    key: String,
    inline_max_bytes: usize,
    extent_bytes: usize,
    buffer: BytesMut,
    /// The body offset of the buffer's first byte.
    offset: u64,
    streaming: bool,
    in_flight: VecDeque<JoinHandle<Result<ExtentRef, ShardError>>>,
    extents: Vec<ExtentRef>,
}

impl<H: Shards> Upload<H> {
    pub(crate) fn new(
        shards: H,
        shard: ShardRef,
        key: String,
        inline_max_bytes: usize,
        extent_bytes: usize,
    ) -> Self {
        Self {
            shards,
            shard,
            key,
            inline_max_bytes,
            extent_bytes: extent_bytes.max(1),
            buffer: BytesMut::new(),
            offset: 0,
            streaming: false,
            in_flight: VecDeque::new(),
            extents: Vec::new(),
        }
    }

    /// Takes the body's next bytes, committing every extent they fill.
    pub(crate) async fn push(&mut self, data: &[u8]) -> S3Result<()> {
        self.buffer.extend_from_slice(data);
        self.streaming |= self.buffer.len() > self.inline_max_bytes;
        while self.streaming && self.buffer.len() >= self.extent_bytes {
            let extent = self.buffer.split_to(self.extent_bytes).freeze();
            self.send(extent).await?;
        }
        Ok(())
    }

    /// Commits what is left of the body, waits until every extent is
    /// applied, and returns where the body's bytes are.
    pub(crate) async fn finish(mut self) -> S3Result<PutData> {
        if !self.streaming {
            return Ok(PutData::Inline(self.buffer.freeze()));
        }
        if !self.buffer.is_empty() {
            let rest = self.buffer.split().freeze();
            self.send(rest).await?;
        }
        while let Some(sent) = self.in_flight.pop_front() {
            self.extents.push(joined(sent).await?);
        }
        Ok(PutData::Extents(self.extents))
    }

    /// Commits one extent, waiting first for the oldest if too many are in
    /// flight. The append continues if the upload is dropped.
    async fn send(&mut self, data: Bytes) -> S3Result<()> {
        if self.in_flight.len() >= MAX_EXTENTS_IN_FLIGHT
            && let Some(oldest) = self.in_flight.pop_front()
        {
            self.extents.push(joined(oldest).await?);
        }
        let extent = Extent {
            key: self.key.clone(),
            offset: self.offset,
            data,
        };
        self.offset += extent.data.len() as u64;
        let (shards, shard) = (self.shards.clone(), self.shard.clone());
        self.in_flight.push_back(tokio::spawn(async move {
            shards.append_extent(&shard, extent).await
        }));
        Ok(())
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
