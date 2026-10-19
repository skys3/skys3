//! Response bodies of a known length, and the parts of a multipart object
//! as one list of extents (§9.2).

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
use crate::shard::{ShardRef, Shards};

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

/// A response body of a known length fed through a channel: by a read
/// from a holder, or by a read-through fill.
pub(super) struct ExtentBody<E> {
    pub(super) receiver: mpsc::Receiver<Result<Bytes, E>>,
    pub(super) remaining: u64,
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
