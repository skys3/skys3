//! The source's end of small-object batches (design §7.8): objects of up
//! to one frame skip staging, and many of them travel in one `BATCH` on a
//! stream of its own, answered with one `APPLIED` per item.
//!
//! ```text
//! source                                        destination
//!   BATCH(items) + inline bytes, finish   ->
//!                                         <-    APPLIED per item, finish
//! ```
//!
//! A whole batch therefore costs one round trip, where staging costs one
//! per object at least: `BEGIN` and its `RESUME`, or a `COMMIT` and its
//! `APPLIED`. [`BatchBuilder`] packs items within the protocol's limits
//! and within one group commit of the destination's log, and
//! [`send_batch`] sends one and collects its answers.

use std::collections::BTreeSet;

use skys3_types::{BucketName, WriteIdentity};

use crate::error::MessageError;
use crate::frame::{MAX_HEADER_LEN, MAX_PAYLOAD_LEN};
use crate::message::{
    Applied, Batch, Commit, MAX_BATCH_ITEMS, Message, Outcome, Put, PutData, Write,
};
use crate::stream::{MessageStream, StreamError};
use crate::wire::batch_item_len;

/// The bytes a `BATCH` header holds besides its items: the envelope's
/// field key and length, and the CRC32C of the payload.
const BATCH_HEADER_OVERHEAD: usize = 16;

/// The log record bytes a batch holds by default: the default
/// `group_commit_max_bytes` (4 MiB). A destination's log takes queued
/// records into one group commit until they reach that size, so a batch
/// within it shares one group commit on a destination with the default
/// settings, and a larger one takes one group commit per 4 MiB (§7.8).
pub const DEFAULT_BATCH_RECORD_BYTES: u64 = 4 << 20;

/// What the destination's log record of an item holds beyond the item's
/// bytes in the `BATCH`: the record's fixed header and framing, and the
/// write identity the destination adds to its metadata, with room to
/// spare.
const RECORD_OVERHEAD: u64 = 256;

/// Whether an object of `size` bytes skips staging and travels in a
/// `BATCH`: it fits in one frame of `frame_bytes` (`peer_frame_bytes`).
/// A delete always does.
///
/// ```
/// use skys3_peer::skips_staging;
///
/// assert!(skips_staging(0, 256 << 10));
/// assert!(skips_staging(256 << 10, 256 << 10));
/// assert!(!skips_staging((256 << 10) + 1, 256 << 10));
/// ```
#[must_use]
pub const fn skips_staging(size: u64, frame_bytes: u64) -> bool {
    size <= frame_bytes
}

/// Why a [`BatchBuilder`] does not take an item.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Refusal {
    /// The batch has no room for it: send the batch, and put the item in
    /// the next one. A builder never refuses the first item of a batch as
    /// full.
    #[error("the batch is full")]
    Full,
    /// The batch already holds the item's key or write identity. A key's
    /// next write waits for the `APPLIED` of the one before (§7.1).
    #[error("the batch already holds the item's key or write identity")]
    Repeated,
    /// The item can never travel in a batch: it carries no inline bytes,
    /// or breaks a rule of the protocol.
    #[error("not a batch item: {0}")]
    Invalid(MessageError),
}

/// Packs small objects and deletes into a `BATCH`, within the protocol's
/// limits: at most [`MAX_BATCH_ITEMS`] items, with distinct keys and
/// distinct write identities, whose inline bytes fit one payload
/// ([`MAX_PAYLOAD_LEN`]) and whose attributes fit one header
/// ([`MAX_HEADER_LEN`]).
///
/// The batch also stays within a budget of log record bytes at the
/// destination, [`DEFAULT_BATCH_RECORD_BYTES`] unless
/// [`BatchBuilder::with_record_bytes`] sets another, so that its records
/// share one group commit there. Each item counts its bytes in the batch
/// plus a fixed overhead, which bounds the record the destination writes
/// for it. An item over the budget by itself travels alone.
///
/// ```
/// use std::collections::BTreeMap;
///
/// use bytes::Bytes;
/// use skys3_peer::{BatchBuilder, Commit, Precondition, Put, PutData, Write};
/// use skys3_types::ETag;
///
/// let mut builder = BatchBuilder::new();
/// for seq in 1..=3u64 {
///     let body = Bytes::from(format!("object {seq}"));
///     let commit = Commit {
///         identity: format!("prod-us/b-7f3a/5/42.{seq}").parse()?,
///         bucket: "archive".parse()?,
///         key: format!("logs/{seq}.txt"),
///         precondition: Precondition::Absent,
///         write: Write::Put(Put {
///             size: body.len() as u64,
///             etag: ETag::new("9e107d9d372bb6826bd81d3542a419d6")?,
///             last_modified_ms: 1_700_000_000_000,
///             metadata: BTreeMap::new(),
///             tags: BTreeMap::new(),
///             checksums: BTreeMap::new(),
///             data: PutData::Inline(body),
///         }),
///     };
///     builder.push(&commit)?;
/// }
/// let batch = builder.take().expect("three items");
/// assert_eq!(batch.items.len(), 3);
/// assert!(builder.is_empty());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct BatchBuilder {
    items: Vec<Commit>,
    keys: BTreeSet<(BucketName, String)>,
    identities: BTreeSet<WriteIdentity>,
    /// The inline bytes of the items.
    payload: u64,
    /// The bytes the items take in the header.
    header: usize,
    /// The record bytes the items are counted for at the destination.
    records: u64,
    /// The most record bytes a batch holds.
    max_records: u64,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::with_record_bytes(DEFAULT_BATCH_RECORD_BYTES)
    }
}

impl BatchBuilder {
    /// An empty batch, within [`DEFAULT_BATCH_RECORD_BYTES`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty batch whose items' records hold at most `max_records`
    /// bytes at the destination, such as its `group_commit_max_bytes`
    /// when the source knows it.
    #[must_use]
    pub fn with_record_bytes(max_records: u64) -> Self {
        Self {
            items: Vec::new(),
            keys: BTreeSet::new(),
            identities: BTreeSet::new(),
            payload: 0,
            header: 0,
            records: 0,
            max_records,
        }
    }

    /// The number of items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch holds no item.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The inline bytes of the items.
    #[must_use]
    pub fn payload_len(&self) -> u64 {
        self.payload
    }

    /// The record bytes the items are counted for at the destination: at
    /// most the builder's budget, unless one item alone exceeds it.
    #[must_use]
    pub fn record_len(&self) -> u64 {
        self.records
    }

    /// Adds `commit`, a delete or a version with its bytes inline.
    ///
    /// # Errors
    ///
    /// The [`Refusal`]; the batch is then unchanged.
    pub fn push(&mut self, commit: &Commit) -> Result<(), Refusal> {
        commit.validate(true).map_err(Refusal::Invalid)?;
        let payload = inline_len(commit);
        if payload > u64::from(MAX_PAYLOAD_LEN) {
            return Err(Refusal::Invalid(MessageError::PayloadTooLong(payload)));
        }
        let header = batch_item_len(commit);
        if header > MAX_HEADER_LEN as usize - BATCH_HEADER_OVERHEAD {
            return Err(Refusal::Invalid(MessageError::HeaderTooLong(header as u64)));
        }
        let key = (commit.bucket.clone(), commit.key.clone());
        if self.keys.contains(&key) || self.identities.contains(&commit.identity) {
            return Err(Refusal::Repeated);
        }
        let records = header as u64 + payload + RECORD_OVERHEAD;
        if self.items.len() == MAX_BATCH_ITEMS
            || self.payload + payload > u64::from(MAX_PAYLOAD_LEN)
            || self.header + header > MAX_HEADER_LEN as usize - BATCH_HEADER_OVERHEAD
            || (!self.items.is_empty() && self.records + records > self.max_records)
        {
            return Err(Refusal::Full);
        }
        self.keys.insert(key);
        self.identities.insert(commit.identity.clone());
        self.payload += payload;
        self.header += header;
        self.records += records;
        self.items.push(commit.clone());
        Ok(())
    }

    /// The batch, if it holds any item, leaving the builder empty with the
    /// same budget.
    pub fn take(&mut self) -> Option<Batch> {
        let builder = std::mem::replace(self, Self::with_record_bytes(self.max_records));
        (!builder.items.is_empty()).then_some(Batch {
            items: builder.items,
        })
    }
}

/// The inline bytes `commit` carries.
fn inline_len(commit: &Commit) -> u64 {
    match &commit.write {
        Write::Put(Put {
            data: PutData::Inline(bytes),
            ..
        }) => bytes.len() as u64,
        Write::Put(_) | Write::Delete => 0,
    }
}

/// Sends `batch` on `stream`, a new stream of its own, finishes it, and
/// returns each item's result, in item order, once the destination has
/// answered every item or ended the stream. The whole batch costs one
/// round trip.
///
/// An item is `None` if no `APPLIED` came for it: whether it applied is
/// unknown, and the source sends it again. A repeated item is answered
/// with the result stored for the first (§7.8). A message that answers no
/// item of the batch is ignored.
///
/// # Errors
///
/// [`StreamError::Protocol`] if the session has no `BATCH` capability, so
/// the source sends each object on its own stream instead; the errors of
/// [`MessageStream::send`] and [`MessageStream::recv`].
pub async fn send_batch(
    stream: &mut MessageStream,
    batch: &Batch,
) -> Result<Vec<Option<Outcome>>, StreamError> {
    stream.send(&Message::Batch(batch.clone())).await?;
    stream.finish()?;
    let mut outcomes: Vec<Option<Outcome>> = batch.items.iter().map(|_| None).collect();
    let mut unanswered = outcomes.len();
    while unanswered > 0 {
        let Some(message) = stream.recv().await? else {
            break;
        };
        let Message::Applied(Applied { identity, outcome }) = message else {
            continue;
        };
        let item = batch
            .items
            .iter()
            .position(|item| item.identity == identity);
        if let Some(slot) = item.map(|at| &mut outcomes[at])
            && slot.is_none()
        {
            *slot = Some(outcome);
            unanswered -= 1;
        }
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use skys3_types::ETag;

    use super::*;
    use crate::frame::parse_prefix;
    use crate::message::Precondition;

    fn put(seq: u64, key: &str, body: Bytes) -> Commit {
        Commit {
            identity: format!("prod-us/b-src/5/42.{seq}").parse().unwrap(),
            bucket: "archive".parse().unwrap(),
            key: key.to_owned(),
            precondition: Precondition::Absent,
            write: Write::Put(Put {
                size: body.len() as u64,
                etag: ETag::new("9e107d9d372bb6826bd81d3542a419d6").unwrap(),
                last_modified_ms: 1_700_000_000_000,
                metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
                tags: BTreeMap::new(),
                checksums: BTreeMap::new(),
                data: PutData::Inline(body),
            }),
        }
    }

    fn delete(seq: u64, key: &str) -> Commit {
        Commit {
            write: Write::Delete,
            ..put(seq, key, Bytes::new())
        }
    }

    /// The length of `batch`'s encoded header.
    fn header_len(batch: Batch) -> usize {
        let frame = Message::Batch(batch).encode().unwrap();
        parse_prefix(frame.first_chunk().unwrap()).unwrap().0
    }

    #[test]
    fn items_are_packed_within_the_count_and_payload_limits() {
        let mut builder = BatchBuilder::new();
        assert_eq!(builder.take(), None);
        for seq in 0..MAX_BATCH_ITEMS as u64 {
            builder.push(&delete(seq, &format!("k{seq}"))).unwrap();
        }
        assert_eq!(builder.len(), MAX_BATCH_ITEMS);
        let next = delete(MAX_BATCH_ITEMS as u64, "next");
        assert_eq!(builder.push(&next), Err(Refusal::Full));
        let batch = builder.take().unwrap();
        assert!(Message::Batch(batch).encode().is_ok());
        builder.push(&next).unwrap();

        // Two 6 MiB objects fit a 16 MiB budget; a third would pass the
        // payload limit.
        let mut builder = BatchBuilder::with_record_bytes(16 << 20);
        let body = Bytes::from(vec![7; 6 << 20]);
        for seq in 0..2 {
            builder
                .push(&put(seq, &format!("big{seq}"), body.clone()))
                .unwrap();
        }
        assert_eq!(builder.payload_len(), 12 << 20);
        assert_eq!(
            builder.push(&put(2, "big2", body.clone())),
            Err(Refusal::Full)
        );
        builder
            .push(&put(2, "small", Bytes::from_static(b"x")))
            .unwrap();
        assert!(Message::Batch(builder.take().unwrap()).encode().is_ok());
    }

    #[test]
    fn items_are_packed_within_the_header_limit() {
        // Items with the most metadata: the header, not the count or the
        // payload, fills first, and every batch still encodes.
        let mut builder = BatchBuilder::with_record_bytes(u64::MAX);
        let mut batches = Vec::new();
        for seq in 0..400u64 {
            let mut item = put(seq, &format!("k{seq}"), Bytes::from_static(b"x"));
            let Write::Put(put) = &mut item.write else {
                unreachable!()
            };
            put.metadata = BTreeMap::from([("x-amz-meta-a".to_owned(), "v".repeat(7900))]);
            match builder.push(&item) {
                Ok(()) => {}
                Err(Refusal::Full) => {
                    batches.push(builder.take().unwrap());
                    builder.push(&item).unwrap();
                }
                Err(other) => panic!("{other}"),
            }
        }
        batches.extend(builder.take());
        assert!(batches.len() > 1, "the header limit splits the items");
        for batch in batches {
            assert!(batch.items.len() < MAX_BATCH_ITEMS);
            assert!(header_len(batch) <= MAX_HEADER_LEN as usize);
        }
    }

    #[test]
    fn batches_stay_within_one_group_commit_of_records() {
        // 1,024 objects of 8 KiB: 8 MiB of records, more than one group
        // commit of the default log holds, so they take three batches.
        let body = Bytes::from(vec![3; 8 << 10]);
        let mut builder = BatchBuilder::new();
        let mut batches = Vec::new();
        for seq in 0..1024 {
            let item = put(seq, &format!("k{seq}"), body.clone());
            if builder.push(&item) == Err(Refusal::Full) {
                assert!(builder.record_len() <= DEFAULT_BATCH_RECORD_BYTES);
                batches.extend(builder.take());
                builder.push(&item).unwrap();
            }
        }
        batches.extend(builder.take());
        assert_eq!(batches.len(), 3);
        assert_eq!(batches.iter().map(|b| b.items.len()).sum::<usize>(), 1024);

        // An item over the budget by itself travels alone.
        let mut builder = BatchBuilder::with_record_bytes(1000);
        builder.push(&put(1, "big", body.clone())).unwrap();
        assert!(builder.record_len() > 1000);
        assert_eq!(builder.push(&delete(2, "small")), Err(Refusal::Full));
        assert_eq!(builder.take().unwrap().items.len(), 1);
        // The next batch keeps the budget.
        builder.push(&delete(2, "small")).unwrap();
        assert_eq!(builder.push(&put(3, "big", body)), Err(Refusal::Full));
    }

    #[test]
    fn the_header_estimate_is_exact_up_to_the_overhead() {
        let items: Vec<_> = (0..50u64)
            .map(|seq| {
                put(
                    seq,
                    &format!("logs/{seq}"),
                    Bytes::from(vec![1; seq as usize]),
                )
            })
            .collect();
        let estimate: usize = items.iter().map(batch_item_len).sum();
        let actual = header_len(Batch { items });
        assert!(actual >= estimate);
        assert!(actual - estimate <= BATCH_HEADER_OVERHEAD);
    }

    #[test]
    fn repeated_and_invalid_items_are_refused() {
        let mut builder = BatchBuilder::new();
        builder
            .push(&put(1, "k", Bytes::from_static(b"a")))
            .unwrap();
        // The same key, or the same identity, waits for the next batch.
        assert_eq!(builder.push(&delete(2, "k")), Err(Refusal::Repeated));
        assert_eq!(builder.push(&delete(1, "other")), Err(Refusal::Repeated));
        // Only inline bytes travel in a batch.
        let mut staged = put(3, "staged", Bytes::new());
        if let Write::Put(put) = &mut staged.write {
            put.data = PutData::Staged { piece: 1 };
        }
        assert!(matches!(builder.push(&staged), Err(Refusal::Invalid(_))));
        // An object larger than a payload never fits.
        let huge = put(
            4,
            "huge",
            Bytes::from(vec![0; MAX_PAYLOAD_LEN as usize + 1]),
        );
        assert_eq!(
            builder.push(&huge),
            Err(Refusal::Invalid(MessageError::PayloadTooLong(
                u64::from(MAX_PAYLOAD_LEN) + 1
            )))
        );
        assert_eq!(builder.len(), 1);
        assert!(Refusal::Full.to_string().contains("full"));
    }
}
