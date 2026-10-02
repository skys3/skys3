//! Response bodies: an object's bytes, or a range of them, read from its
//! shard (§9.2).

use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Frame, SizeHint};
use s3s::dto::StreamingBlob;
use s3s::{S3Result, s3_error};
use skys3_index::{ObjectPart, Payload};
use skys3_log::record::ExtentRef;
use skys3_types::EpochSeq;
use tokio::sync::mpsc;

use crate::buckets::shard_error;
use crate::fill::FillBody;
use crate::shard::{ShardError, ShardRef, Shards};

/// The bytes `range` of an object whose bytes are `payload`.
///
/// Inline bytes are read before the response starts. Extents are read one
/// at a time by a task that stays at most one extent ahead of the client,
/// and stops when the response is dropped. An object's payload never
/// changes, so the read needs no lock: every position it names stays
/// readable until compaction, which keeps what a live entry names (§10.3).
pub(crate) async fn read<H: Shards>(
    shards: &H,
    shard: &ShardRef,
    payload: &Payload,
    range: Range<u64>,
) -> S3Result<StreamingBlob> {
    match payload {
        Payload::Inline(position) => {
            let data = shards
                .payload(shard, *position)
                .await
                .map_err(shard_error)?;
            let slice = usize::try_from(range.start)
                .ok()
                .zip(usize::try_from(range.end).ok())
                .filter(|(_, end)| *end <= data.len())
                .ok_or_else(|| {
                    s3_error!(InternalError, "the stored object is shorter than its size")
                })?;
            Ok(StreamingBlob::from_bytes(data.slice(slice.0..slice.1)))
        }
        Payload::Extents(extents) => Ok(stream(shards, shard, extents.clone(), range)),
        Payload::Parts { upload, parts } => {
            let (extents, range) = part_extents(shards, shard, *upload, parts, range).await?;
            Ok(stream(shards, shard, extents, range))
        }
        Payload::None => Err(not_cached()),
    }
}

/// The answer to a read of an object whose bytes this cluster does not
/// hold and cannot fill: an evicted version outside a `write_back` bucket
/// with a [`Fills`](crate::fill::Fills), or the source of a copy.
pub(super) fn not_cached() -> s3s::S3Error {
    s3_error!(
        ServiceUnavailable,
        "The object's bytes are not cached on this cluster"
    )
}

/// The response body of `remaining` bytes that a read-through fill streams
/// (§9.2).
pub(super) fn filled(body: FillBody, remaining: u64) -> StreamingBlob {
    StreamingBlob::from(s3s::Body::http_body(ExtentBody {
        receiver: body,
        remaining,
    }))
}

/// Streams the bytes `range` of the body made of `extents`.
fn stream<H: Shards>(
    shards: &H,
    shard: &ShardRef,
    extents: Vec<ExtentRef>,
    range: Range<u64>,
) -> StreamingBlob {
    let (sender, receiver) = mpsc::channel(1);
    let remaining = range.end - range.start;
    tokio::spawn(send_extents(
        shards.clone(),
        shard.clone(),
        extents,
        range,
        sender,
    ));
    StreamingBlob::from(s3s::Body::http_body(ExtentBody {
        receiver,
        remaining,
    }))
}

/// The bytes of the parts of a multipart object that `range` overlaps, as
/// one list of extents, and `range` within them.
///
/// The parts are read from the index in one transaction before the
/// response starts: a later write of the key drops them, and a read that
/// resolved the object first still finds what it needs (§10.3). An inline
/// part is one extent, its `MPU_PART` record.
pub(super) async fn part_extents<H: Shards>(
    shards: &H,
    shard: &ShardRef,
    upload: EpochSeq,
    parts: &[ObjectPart],
    range: Range<u64>,
) -> S3Result<(Vec<ExtentRef>, Range<u64>)> {
    // The parts `range` overlaps, and where the first of them starts.
    let mut offset = 0;
    let mut start = None;
    let mut needed = Vec::new();
    for part in parts {
        let end = offset + part.size;
        if end > range.start && offset < range.end {
            start.get_or_insert(offset);
            needed.push(*part);
        }
        offset = end;
    }
    let (Some(start), Some(first)) = (start, needed.first()) else {
        return Ok((Vec::new(), 0..0));
    };
    let rows = shards
        .parts(shard, upload, first.number - 1, needed.len())
        .await
        .map_err(shard_error)?;
    let found: Vec<_> = rows.iter().map(|(n, part)| (*n, part.size)).collect();
    let expected: Vec<_> = needed.iter().map(|part| (part.number, part.size)).collect();
    if found != expected {
        // The key was written again since the read resolved it.
        return Err(s3_error!(
            ServiceUnavailable,
            "The object changed while it was read; please retry"
        ));
    }
    let mut extents = Vec::new();
    for (_, part) in rows {
        match part.payload {
            Payload::Inline(position) => {
                if let Ok(len) = u32::try_from(part.size)
                    && len > 0
                {
                    extents.push(ExtentRef { position, len });
                }
            }
            Payload::Extents(part_extents) => extents.extend(part_extents),
            // The object was evicted since the read resolved it, or is a
            // copy of an evicted one.
            Payload::None => return Err(not_cached()),
            Payload::Parts { .. } => {
                return Err(s3_error!(InternalError, "a part has no bytes"));
            }
        }
    }
    Ok((extents, range.start - start..range.end - start))
}

/// Reads the parts of `extents` that overlap `range`, in order, into
/// `sender`, until the receiver is gone or a read fails.
async fn send_extents<H: Shards>(
    shards: H,
    shard: ShardRef,
    extents: Vec<ExtentRef>,
    range: Range<u64>,
    sender: mpsc::Sender<Result<Bytes, ShardError>>,
) {
    let mut start = 0;
    for extent in extents {
        let end = start + u64::from(extent.len);
        if end > range.start && start < range.end {
            let read = shards
                .payload(&shard, extent.position)
                .await
                .and_then(|data| clip(&shard, &data, &extent, start, &range));
            let failed = read.is_err();
            if sender.send(read).await.is_err() || failed {
                return;
            }
        }
        if end >= range.end {
            return;
        }
        start = end;
    }
}

/// The part of `data`, the extent at body offset `start`, inside `range`.
fn clip(
    shard: &ShardRef,
    data: &Bytes,
    extent: &ExtentRef,
    start: u64,
    range: &Range<u64>,
) -> Result<Bytes, ShardError> {
    if data.len() as u64 != u64::from(extent.len) {
        return Err(ShardError::Unavailable {
            shard: shard.clone(),
            reason: format!("the extent at {} has the wrong length", extent.position),
        });
    }
    // Both bounds are within the extent, so they fit in `usize`.
    let from = range.start.saturating_sub(start) as usize;
    let to = (range.end - start).min(u64::from(extent.len)) as usize;
    Ok(data.slice(from..to))
}

/// A response body of a known length fed through a channel: by
/// [`send_extents`], or by a read-through fill.
struct ExtentBody<E> {
    receiver: mpsc::Receiver<Result<Bytes, E>>,
    remaining: u64,
}

impl<E> http_body::Body for ExtentBody<E> {
    type Data = Bytes;
    type Error = E;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, E>>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        self.receiver.poll_recv(cx).map(|next| {
            next.map(|read| {
                read.map(|data| {
                    self.remaining = self.remaining.saturating_sub(data.len() as u64);
                    Frame::data(data)
                })
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}
