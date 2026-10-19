//! Staging an object at the destination (§7.8): `BEGIN`, the `RESUME`
//! that says what the destination holds durably, `DATA` for the rest, read
//! from the local log, and the `COMMIT` on the same stream.
//!
//! The destination stages each frame where it lacks the bytes, and its
//! `RESUME` reports what is durable on every member of its shard, so a
//! link that drops, or a flusher that stops, costs only what was in
//! flight: the next `BEGIN` of the same identity, from this flusher or
//! another primary's, resumes from the durable ranges.

use bytes::Bytes;
use skys3_index::{ObjectPart, Payload};
use skys3_io::Disk;
use skys3_log::record::ExtentRef;
use skys3_peer::{AbortReason, ApplyError, Begin, ByteRanges, Commit, Data, Message, Outcome};
use skys3_shard::Shard;
use skys3_types::{EpochSeq, WriteIdentity};

use super::batch::assumed_committed;
use super::hooks::peer_bug;
use super::link::{LinkError, Stream};
use super::{Native, PIECE, PeerBug};
use crate::attempt::Failure;
use crate::target::Target;

/// How many times one flush stages an object again after the destination
/// answered that its staging is incomplete or gone.
const STAGE_ROUNDS: usize = 3;

/// One run of a version's bytes in the local log: `len` bytes at `offset`
/// of the object, the whole payload of the record at `position`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Segment {
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) position: EpochSeq,
}

/// Where bytes of an object are in the local log, in object order. A
/// committed version's layout covers it whole; a streamed body's holds the
/// extents announced so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Layout(pub(crate) Vec<Segment>);

impl Layout {
    /// The layout of a body's announced `extents`, by offset.
    pub(crate) fn of_extents<'e>(
        extents: impl IntoIterator<Item = (&'e u64, &'e ExtentRef)>,
    ) -> Self {
        Self(
            extents
                .into_iter()
                .map(|(&offset, extent)| Segment {
                    offset,
                    len: u64::from(extent.len),
                    position: extent.position,
                })
                .collect(),
        )
    }

    /// The layout of a version of `size` bytes stored as `payload` in
    /// `shard`: its own record, its extents, or its parts' records and
    /// extents, in part order.
    pub(crate) async fn of_version<D: Disk>(
        shard: &Shard<D>,
        payload: &Payload,
        size: u64,
    ) -> Result<Self, Failure> {
        let mut segments = Vec::new();
        let mut offset = 0;
        let mut add = |len: u64, position: EpochSeq| {
            segments.push(Segment {
                offset,
                len,
                position,
            });
            offset += len;
        };
        match payload {
            Payload::Inline(position) => add(size, *position),
            Payload::Extents(extents) => {
                for extent in extents {
                    add(u64::from(extent.len), extent.position);
                }
            }
            Payload::Parts { upload, parts } => {
                for (_, part) in parts_of(shard, *upload, parts).await? {
                    match part.payload {
                        Payload::Inline(position) => add(part.size, position),
                        Payload::Extents(extents) => {
                            for extent in extents {
                                add(u64::from(extent.len), extent.position);
                            }
                        }
                        Payload::None | Payload::Parts { .. } => {
                            return Err(Failure::Local("a part has no local bytes".into()));
                        }
                    }
                }
            }
            // An imported or evicted stub changed by `TAGS`: its bytes must
            // be filled from the remote first (plan M1-20).
            Payload::None => return Err(Failure::Local("the version has no local bytes".into())),
        }
        if offset != size {
            return Err(Failure::Local(format!(
                "the version's bytes are {offset} long, not {size}"
            )));
        }
        Ok(Self(segments))
    }
}

/// The parts of the upload at `upload`, checked against the object's
/// `layout`.
async fn parts_of<D: Disk>(
    shard: &Shard<D>,
    upload: EpochSeq,
    layout: &[ObjectPart],
) -> Result<Vec<(u16, skys3_index::Part)>, Failure> {
    let parts = shard.parts(upload, 0, layout.len()).await?;
    let complete = parts.len() == layout.len()
        && parts
            .iter()
            .zip(layout)
            .all(|((number, part), listed)| *number == listed.number && part.size == listed.size);
    if complete {
        Ok(parts)
    } else {
        Err(Failure::Local(format!(
            "the parts of upload {upload} are not in the index as the object lists them"
        )))
    }
}

/// What a stream that staged bytes ended with.
pub(crate) enum Ended {
    /// The destination answered the `COMMIT`.
    Applied(Outcome),
    /// The destination discarded the staging, for this reason.
    Aborted(AbortReason, String),
}

/// Stages bytes of one write identity at the destination, on one stream.
pub(crate) struct Staged<'a, S, D: Disk> {
    pub(crate) shard: &'a Shard<D>,
    pub(crate) target: &'a Target<S>,
    pub(crate) native: &'a Native,
    pub(crate) begin: Begin,
}

impl<S, D: Disk> Staged<'_, S, D> {
    /// Opens a stream and sends `BEGIN`, and returns the stream with what
    /// its `RESUME` says the destination holds durably. An `ABORT` instead
    /// is the `Err`'s `Ended`.
    pub(crate) async fn open(&self) -> Result<Result<(Stream, ByteRanges), Ended>, LinkError> {
        let mut stream = Stream::open(&*self.native.link, self.native.timeout).await?;
        stream.send(&Message::Begin(self.begin.clone())).await?;
        loop {
            match stream.recv().await? {
                Some(Message::Resume(resume)) if resume.identity == self.begin.identity => {
                    let durable = resume.pieces.get(&PIECE).cloned().unwrap_or_default();
                    return Ok(Ok((stream, durable)));
                }
                Some(Message::Abort(abort)) if abort.identity == self.begin.identity => {
                    return Ok(Err(Ended::Aborted(abort.reason, abort.detail)));
                }
                Some(_) => {}
                None => return Err(LinkError::new("the destination ended the stream")),
            }
        }
    }

    /// Sends the bytes of `layout` that `durable` lacks, frame by frame,
    /// each read once from the local log within the target's in-flight
    /// budget.
    pub(crate) async fn send(
        &self,
        stream: &mut Stream,
        layout: &Layout,
        durable: &ByteRanges,
    ) -> Result<(), Failure> {
        let frame = self.native.frame_bytes;
        let nothing = ByteRanges::default();
        let durable = if peer_bug() == PeerBug::ResendDurable {
            &nothing
        } else {
            durable
        };
        for segment in &layout.0 {
            let end = segment.offset + segment.len;
            let wanted: Vec<_> = durable
                .missing(end)
                .into_iter()
                .filter_map(|gap| {
                    let (start, stop) = (gap.start.max(segment.offset), gap.end.min(end));
                    (start < stop).then_some(start..stop)
                })
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let _reserved = self.target.reserve(segment.len).await;
            let bytes = self.read(segment).await?;
            for range in wanted {
                let mut offset = range.start;
                while offset < range.end {
                    let len = frame.min(range.end - offset);
                    let at = usize::try_from(offset - segment.offset).unwrap_or(usize::MAX);
                    let data = Data {
                        piece: PIECE,
                        offset,
                        bytes: bytes.slice(at..at + usize::try_from(len).unwrap_or(usize::MAX)),
                    };
                    stream
                        .send(&Message::Data(data))
                        .await
                        .map_err(Failure::link)?;
                    offset += len;
                }
            }
        }
        Ok(())
    }

    /// The bytes of `segment`, from its record.
    async fn read(&self, segment: &Segment) -> Result<Bytes, Failure> {
        let bytes = self.shard.payload(segment.position).await?;
        if bytes.len() as u64 == segment.len {
            Ok(bytes)
        } else {
            Err(Failure::Local(format!(
                "the record at {} holds {} bytes, not {}",
                segment.position,
                bytes.len(),
                segment.len
            )))
        }
    }

    /// Stages what the destination lacks of `layout`, the whole version,
    /// and commits it with `commit` on the same stream; `sent` is called
    /// once the `COMMIT` is on its way. Staging that the destination
    /// reports incomplete or discarded is sent again, a few times.
    pub(crate) async fn commit(
        &self,
        commit: &Commit,
        layout: &Layout,
        sent: &(dyn Fn() + Sync),
    ) -> Result<Outcome, Failure> {
        let identity = &self.begin.identity;
        for _ in 0..STAGE_ROUNDS {
            let (mut stream, durable) = match self.open().await.map_err(Failure::link)? {
                Ok(opened) => opened,
                Err(ended) => {
                    refused(&ended)?;
                    if expired_as_committed(&ended) {
                        return Ok(assumed_committed(commit));
                    }
                    continue;
                }
            };
            self.send(&mut stream, layout, &durable).await?;
            stream
                .send(&Message::Commit(self.native.stamped(commit)))
                .await
                .map_err(Failure::link)?;
            stream.finish().map_err(Failure::link)?;
            sent();
            if peer_bug() == PeerBug::FlushedOnCommit {
                return Ok(assumed_committed(commit));
            }
            let ended = applied(&mut stream, identity)
                .await
                .map_err(Failure::link)?;
            refused(&ended)?;
            if expired_as_committed(&ended) {
                return Ok(assumed_committed(commit));
            }
            match ended {
                Ended::Applied(Outcome::Failed {
                    error: ApplyError::Incomplete,
                    ..
                })
                | Ended::Aborted(..) => {}
                Ended::Applied(outcome) => return Ok(outcome),
            }
        }
        Err(Failure::Peer(format!(
            "the destination found the staging of {identity} incomplete {STAGE_ROUNDS} times"
        )))
    }
}

/// Whether the seeded bug [`PeerBug::ExpiredAsCommitted`] takes `ended`,
/// staging found incomplete or expired, for a commit.
fn expired_as_committed(ended: &Ended) -> bool {
    peer_bug() == PeerBug::ExpiredAsCommitted
        && matches!(
            ended,
            Ended::Applied(Outcome::Failed {
                error: ApplyError::Incomplete,
                ..
            }) | Ended::Aborted(AbortReason::Expired, _)
        )
}

/// Fails with the destination's refusal if `ended` is one: staging again
/// cannot help.
fn refused(ended: &Ended) -> Result<(), Failure> {
    match ended {
        Ended::Aborted(AbortReason::Refused, detail) => Err(Failure::Peer(format!(
            "the destination refused the staging: {detail}"
        ))),
        _ => Ok(()),
    }
}

/// Reads `stream` until the destination answers the `COMMIT` of
/// `identity` or discards its staging.
pub(crate) async fn applied(
    stream: &mut Stream,
    identity: &WriteIdentity,
) -> Result<Ended, LinkError> {
    loop {
        match stream.recv().await? {
            Some(Message::Applied(applied)) if applied.identity == *identity => {
                return Ok(Ended::Applied(applied.outcome));
            }
            Some(Message::Abort(abort)) if abort.identity == *identity => {
                return Ok(Ended::Aborted(abort.reason, abort.detail));
            }
            Some(_) => {}
            None => return Err(LinkError::new("the stream ended without an APPLIED")),
        }
    }
}
