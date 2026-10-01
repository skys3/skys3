//! Response bodies: an object's bytes, or a range of them, read from its
//! shard (§9.2).

use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Frame, SizeHint};
use s3s::dto::StreamingBlob;
use s3s::{S3Result, s3_error};
use skys3_index::Payload;
use skys3_log::record::ExtentRef;
use tokio::sync::mpsc;

use crate::buckets::shard_error;
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
        Payload::Extents(extents) => {
            let (sender, receiver) = mpsc::channel(1);
            tokio::spawn(send_extents(
                shards.clone(),
                shard.clone(),
                extents.clone(),
                range.clone(),
                sender,
            ));
            let body = ExtentBody {
                receiver,
                remaining: range.end - range.start,
            };
            Ok(StreamingBlob::from(s3s::Body::http_body(body)))
        }
        Payload::None => Err(not_cached()),
    }
}

/// The answer to a read of an object whose bytes this cluster does not
/// hold, until read-through fill (plan M1-20) fetches them.
pub(super) fn not_cached() -> s3s::S3Error {
    s3_error!(
        ServiceUnavailable,
        "The object's bytes are not cached on this cluster"
    )
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

/// A response body fed by [`send_extents`], of a known length.
struct ExtentBody {
    receiver: mpsc::Receiver<Result<Bytes, ShardError>>,
    remaining: u64,
}

impl http_body::Body for ExtentBody {
    type Data = Bytes;
    type Error = ShardError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ShardError>>> {
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
