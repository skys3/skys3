//! The flush of a streamed single PUT (§7.3, §7.4): the completion of the
//! remote multipart upload its body streamed to while the client uploaded
//! (see the `stream` module).
//!
//! The `PUT` names the body's bytes; part *n* is its bytes from `(n−1)·P`
//! to `n·P`, where `P` is the stream's `flush_part_bytes`, and the last
//! part is shorter. A part the stream sent from exactly the extents the
//! `PUT` names in its range is listed as the remote holds it. Every other
//! part is sent now from the `PUT`'s extents, with the `Content-MD5` of its
//! bytes. The completion carries the §7.2 precondition, conditioned like
//! any flush on the `remote_etag` of the version the remote holds.
//!
//! The remote ETag of the object is then a multipart ETag, which the
//! `FLUSHED` records as the entry's `remote_etag`: later flushes of the key
//! are conditioned on it, and fills read the object with it. The entry
//! keeps the MD5 `local_etag` that clients see (§7.4).

use std::collections::BTreeMap;

use skys3_index::{ObjectVersion, Payload};
use skys3_io::Disk;
use skys3_log::record::ExtentRef;
use skys3_remote::{CompletedPart, ObjectStore};
use skys3_types::{ETag, EpochSeq, WriteIdentity};

use crate::attempt::{Attempt, Failure, Outcome};
use crate::multipart::verdict_after;
use crate::stream::{self, Claim, Verdict, part_range, span};

/// The most parts a remote multipart upload may have (S3's limit).
const MAX_PARTS: u64 = 10_000;

impl<S: ObjectStore, D: Disk> Attempt<'_, S, D> {
    /// Whether `object`, whose identity names the `UPLOAD_BEGIN` at
    /// `upload`, can complete a remote upload of its body: it has bytes,
    /// held in this shard's log, in at most [`MAX_PARTS`] parts. If not,
    /// it is sent as one `PutObject`, and its stream is aborted once the key
    /// is clean.
    pub(crate) fn streamable(&self, object: &ObjectVersion) -> bool {
        let part_bytes = self.target.settings.part_bytes.max(1);
        object.size > 0
            && object.size.div_ceil(part_bytes) <= MAX_PARTS
            && body_extents(&object.payload, object.size).is_some()
    }

    /// Completes the stream of the body begun at `upload`, which the
    /// attempt claimed, as the single PUT `object` at `version`,
    /// conditioned on `expected` (§7.2, §7.3).
    pub(crate) async fn complete_body(
        &self,
        version: EpochSeq,
        object: &ObjectVersion,
        upload: EpochSeq,
        claim: Claim,
        identity: &WriteIdentity,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let streams = self.streams;
        let (part_bytes, spans) = claim
            .body
            .unwrap_or_else(|| (self.target.settings.part_bytes, BTreeMap::new()));
        let part_bytes = part_bytes.max(1);
        let count = object.size.div_ceil(part_bytes);
        let extents = match body_extents(&object.payload, object.size) {
            Some(extents) if (1..=MAX_PARTS).contains(&count) => extents,
            _ => {
                streams.release(upload, Verdict::Abort);
                return Err(Failure::Local(
                    "the object cannot complete its streamed upload".into(),
                ));
            }
        };
        let mut completed = Vec::new();
        let mut streamed = 0;
        for number in (1..).take_while(|n| u64::from(*n) <= count) {
            let (start, end) = part_range(number, part_bytes);
            let range = (start, end.min(object.size));
            let Some(span) = span(&extents, range) else {
                streams.release(upload, Verdict::Keep);
                return Err(Failure::Local(format!(
                    "the object's extents do not hold bytes {} to {}",
                    range.0, range.1
                )));
            };
            let held = claim
                .flushed
                .get(&number)
                .filter(|_| spans.get(&number) == Some(&span));
            let etag = match held {
                Some((position, etag)) => {
                    let sent_before = claim.at_completion.as_ref();
                    if sent_before.is_some_and(|parts| parts.get(&number) == Some(position)) {
                        streamed += range.1 - range.0;
                    }
                    etag.clone()
                }
                None => {
                    let sent = stream::upload_span(
                        self.shard,
                        self.target,
                        self.key,
                        &claim.id,
                        number,
                        range,
                        &span,
                    )
                    .await;
                    match sent {
                        Ok(etag) => {
                            streams.sent_span(upload, number, span, etag.clone());
                            etag
                        }
                        Err(failure) => {
                            streams.release(upload, verdict_after(&failure));
                            return Err(failure);
                        }
                    }
                }
            };
            completed.push(CompletedPart {
                part_number: u32::from(number),
                etag,
            });
        }
        if claim.at_completion.is_some() {
            #[allow(clippy::cast_precision_loss, reason = "a ratio")]
            let overlap = streamed as f64 / object.size as f64;
            self.target.counters.streaming_overlap.observe(overlap);
        }
        self.complete_claimed(version, upload, &claim.id, completed, identity, expected)
            .await
    }
}

/// The extents that hold a version's bytes, by offset: those it names, or
/// its inline bytes as one. `None` for a version without bytes in the log,
/// or whose extents do not add up to its size.
fn body_extents(payload: &Payload, size: u64) -> Option<BTreeMap<u64, ExtentRef>> {
    let mut extents = BTreeMap::new();
    match payload {
        Payload::Inline(position) => {
            let len = u32::try_from(size).ok()?;
            extents.insert(
                0,
                ExtentRef {
                    position: *position,
                    len,
                },
            );
        }
        Payload::Extents(refs) => {
            let mut offset = 0u64;
            for extent in refs {
                extents.insert(offset, *extent);
                offset = offset.checked_add(u64::from(extent.len))?;
            }
            if offset != size {
                return None;
            }
        }
        Payload::None | Payload::Parts { .. } => return None,
    }
    Some(extents)
}

#[cfg(test)]
mod tests {
    use skys3_index::ObjectPart;
    use skys3_types::{Epoch, Seq};

    use super::*;

    fn at(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    #[test]
    fn a_body_is_its_extents_or_its_inline_bytes() {
        let extents = Payload::Extents(vec![
            ExtentRef {
                position: at(1),
                len: 4,
            },
            ExtentRef {
                position: at(2),
                len: 2,
            },
        ]);
        let found = body_extents(&extents, 6).unwrap();
        assert_eq!(found.keys().copied().collect::<Vec<_>>(), [0, 4]);
        assert_eq!(body_extents(&extents, 7), None);
        let inline = body_extents(&Payload::Inline(at(3)), 5).unwrap();
        assert_eq!(inline[&0].len, 5);
        assert_eq!(body_extents(&Payload::Inline(at(3)), u64::MAX), None);
        assert_eq!(body_extents(&Payload::None, 5), None);
        let parts = Payload::Parts {
            upload: at(4),
            parts: vec![ObjectPart { number: 1, size: 5 }],
        };
        assert_eq!(body_extents(&parts, 5), None);
    }
}
