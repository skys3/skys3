//! Reads of coded objects (design §8.5): a range of an object version,
//! read from the fragments its coded layout names.
//!
//! The code is systematic, so a healthy read fetches only the data
//! fragments that cover the range, with no decoding. The range is cut
//! into *pieces*, each within one data fragment of one stripe and at most
//! [`PIECE_LEN`] bytes long, fetched one after another: the first before
//! [`read_coded`] returns, so a read that cannot start fails before a
//! response does, and the rest at most one piece ahead of the consumer.
//!
//! A piece whose fragment cannot be read (its node does not answer, does
//! not hold it, or holds it damaged) or whose bytes fail the CRC32C they
//! came with is *degraded*: the reader fetches the same bytes of `k` other
//! fragments of the stripe, the 64-byte columns that cover the piece
//! ([`EcCodec::columns`]), and decodes just those
//! ([`EcCodec::decode_columns`]). It asks the stripe's other fragments in
//! index order, data fragments first, and replaces any that fail with the
//! next, so a stripe that lost up to `m` fragments, missing or corrupt,
//! still reads. The fragments that failed are not asked again for the
//! rest of the stripe, and a node that did not answer is not asked again
//! for the rest of the read.
//!
//! Every fragment is read by its node and ID together with what its header
//! must say: the shard, key, and version, the stripe, and the fragment's
//! index ([`FragmentIdentity`]). A node serves a fragment only if its
//! header matches, so a fragment reclaimed since the plan, or an ID reused
//! on a replaced disk, reads as missing, never as other bytes.
//!
//! Fragments travel on the intra-cluster transport ([`FragmentReadClient`],
//! served by [`FragmentServer::serve_reads`]); a [`FragmentSource`] is what
//! the reader asks, so tests can stand in for the nodes.
//!
//! [`FragmentServer::serve_reads`]: crate::FragmentServer::serve_reads

mod wire;

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use skys3_log::ShardRef;
use skys3_types::{CodedStripe, EpochSeq, FragmentId, NodeId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::fragment::{FragmentHeader, StripeInfo};
use crate::{EcCodec, EcError, codec};

pub use wire::{FragmentData, FragmentRead, FragmentReadClient, MAX_READ_LEN};

/// The most bytes of one fragment a piece of a read covers. A degraded
/// piece holds `k` such ranges in memory while it decodes.
pub const PIECE_LEN: u64 = 1 << 20;

/// What a fragment's header must say for a read of it to be served: the
/// object version, the stripe, and the fragment's index in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentIdentity {
    /// The object's shard.
    pub shard: ShardRef,
    /// The object's key.
    pub key: String,
    /// The position of the record that committed the version.
    pub version: EpochSeq,
    /// The stripe, as every fragment of it describes it.
    pub stripe: StripeInfo,
    /// The fragment's index within the stripe.
    pub index: u8,
}

impl FragmentIdentity {
    /// Whether `header` is the header of the fragment this names. The
    /// attempt that wrote it does not matter: fragments of a stripe that
    /// agree on its layout are interchangeable (§8.4).
    #[must_use]
    pub fn matches(&self, header: &FragmentHeader) -> bool {
        header.shard == self.shard
            && header.key == self.key
            && header.version == self.version
            && header.stripe == self.stripe
            && header.index == self.index
    }
}

/// A read of bytes `range` of fragment `fragment` on node `node`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRequest {
    /// The node that holds the fragment.
    pub node: NodeId,
    /// The fragment's ID on that node.
    pub fragment: FragmentId,
    /// What the fragment's header must say.
    pub identity: FragmentIdentity,
    /// The bytes of the fragment to read.
    pub range: Range<u64>,
}

/// The bytes a fragment read returned, with the CRC32C their node computed
/// from the bytes it verified, which the reader checks after the transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentBytes {
    /// The bytes.
    pub data: Bytes,
    /// Their CRC32C.
    pub crc32c: u32,
}

/// Why a fragment could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentReadError {
    /// The node answered that it holds no such fragment: none under the
    /// ID, or one whose header names another fragment.
    #[error("node {node} does not hold the fragment: {reason}")]
    NotHeld {
        /// The node.
        node: NodeId,
        /// Its reason.
        reason: String,
    },
    /// The node holds the fragment but could not read it intact, or the
    /// bytes it sent fail their checks.
    #[error("the fragment on node {node} is damaged: {reason}")]
    Damaged {
        /// The node.
        node: NodeId,
        /// What failed.
        reason: String,
    },
    /// The node could not be asked, or did not answer.
    #[error("node {node} did not answer: {reason}")]
    Unreachable {
        /// The node.
        node: NodeId,
        /// Why.
        reason: String,
    },
}

/// A future of [`FragmentSource::read`].
pub type ReadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FragmentBytes, FragmentReadError>> + Send + 'a>>;

/// What a coded read fetches fragments from: [`FragmentReadClient`] over
/// the cluster transport, or a test's stand-in. The method returns a boxed
/// future so a gateway can hold any source behind one `Arc`.
pub trait FragmentSource: fmt::Debug + Send + Sync + 'static {
    /// Reads `request`'s bytes from its node.
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_>;
}

/// A read of bytes `range` of a coded object version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodedRead {
    /// The object's shard.
    pub shard: ShardRef,
    /// The object's key.
    pub key: String,
    /// The position of the record that committed the version.
    pub version: EpochSeq,
    /// The object's size.
    pub size: u64,
    /// The version's stripes, as its coded layout gives them.
    pub stripes: Vec<CodedStripe>,
    /// The bytes to read, within the object.
    pub range: Range<u64>,
}

/// Why a coded read failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CodedReadError {
    /// The layout does not cover the object exactly once, or the range is
    /// not within the object.
    #[error("the coded layout cannot serve the read: {0}")]
    Layout(String),
    /// A stripe's codec is unknown or rejects the layout.
    #[error("stripe {stripe} cannot be decoded: {source}")]
    Codec {
        /// The stripe's number.
        stripe: u32,
        /// What the codec said.
        source: EcError,
    },
    /// Fewer than `k` fragments of a stripe could be read.
    #[error("stripe {stripe} has {readable} of the {needed} fragments it needs readable")]
    Unreadable {
        /// The stripe's number.
        stripe: u32,
        /// The fragments that could be read.
        readable: usize,
        /// `k`.
        needed: usize,
    },
}

/// The bytes of a coded read, in order. An error ends the stream: the read
/// failed after it began.
pub type CodedBody = mpsc::Receiver<io::Result<Bytes>>;

/// Reads `read`'s range from the fragments its stripes name, through
/// `source`. Returns once the first piece is read; the body streams the
/// rest, read at most one piece ahead. An empty range returns an empty
/// body.
///
/// # Errors
///
/// [`CodedReadError::Layout`] if the stripes do not cover the object or
/// the range is outside it, and the errors of reading the first piece.
pub async fn read_coded(
    source: Arc<dyn FragmentSource>,
    read: CodedRead,
) -> Result<CodedBody, CodedReadError> {
    let pieces = pieces(&read)?;
    let mut reader = Reader {
        source,
        stripe_count: read.stripes.len() as u32,
        read,
        bad: BTreeSet::new(),
        unreachable: BTreeSet::new(),
    };
    let mut pieces = pieces.into_iter();
    let first = match pieces.next() {
        Some(piece) => Some(reader.piece(&piece).await?),
        None => None,
    };
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        let Some(first) = first else { return };
        if sender.send(Ok(first)).await.is_err() {
            return;
        }
        for piece in pieces {
            let read = reader.piece(&piece).await.map_err(io::Error::other);
            let failed = read.is_err();
            if sender.send(read).await.is_err() || failed {
                return;
            }
        }
    });
    Ok(receiver)
}

/// Bytes `within` of data fragment `fragment` of stripe `stripe` (an
/// index into [`CodedRead::stripes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Piece {
    stripe: usize,
    fragment: usize,
    within: Range<u64>,
}

/// The pieces of `read`'s range, in order, after checking that its stripes
/// cover the object exactly once and the range lies within it.
fn pieces(read: &CodedRead) -> Result<Vec<Piece>, CodedReadError> {
    let range = &read.range;
    if range.start > range.end || range.end > read.size {
        return Err(CodedReadError::Layout(format!(
            "bytes {range:?} of an object of {} bytes",
            read.size
        )));
    }
    let mut pieces = Vec::new();
    let mut end = 0;
    for (n, stripe) in read.stripes.iter().enumerate() {
        if stripe.number() as usize != n || stripe.offset() != end {
            return Err(CodedReadError::Layout(format!(
                "stripe {} at {} does not follow byte {end}",
                stripe.number(),
                stripe.offset()
            )));
        }
        end = stripe.end();
        let start = range.start.max(stripe.offset());
        let stop = range.end.min(stripe.end());
        if start >= stop {
            continue;
        }
        let codec = codec_of(stripe)?;
        let fragment_len = codec
            .fragment_len(stripe.geometry(), stripe.data_len())
            .map_err(|source| CodedReadError::Codec {
                stripe: stripe.number(),
                source,
            })?;
        // Offsets within the stripe's data, which data fragment `i` holds
        // from `i * fragment_len`.
        let mut at = start - stripe.offset();
        let stop = stop - stripe.offset();
        while at < stop {
            let fragment = at / fragment_len;
            let within = at % fragment_len;
            let len = (fragment_len - within).min(stop - at).min(PIECE_LEN);
            pieces.push(Piece {
                stripe: n,
                // Below `k`, at most 255.
                fragment: fragment as usize,
                within: within..within + len,
            });
            at += len;
        }
    }
    if end != read.size {
        return Err(CodedReadError::Layout(format!(
            "the stripes end at byte {end} of an object of {} bytes",
            read.size
        )));
    }
    Ok(pieces)
}

fn codec_of(stripe: &CodedStripe) -> Result<&'static dyn EcCodec, CodedReadError> {
    codec(stripe.codec()).map_err(|source| CodedReadError::Codec {
        stripe: stripe.number(),
        source,
    })
}

/// One coded read in progress: what it learned of the fragments so far.
struct Reader {
    source: Arc<dyn FragmentSource>,
    read: CodedRead,
    stripe_count: u32,
    /// Fragments that failed, by stripe and index: not asked again.
    bad: BTreeSet<(usize, usize)>,
    /// Nodes that did not answer: not asked again during this read.
    unreachable: BTreeSet<NodeId>,
}

impl Reader {
    /// The bytes of `piece`: from its data fragment, or decoded from `k`
    /// others if that fails.
    async fn piece(&mut self, piece: &Piece) -> Result<Bytes, CodedReadError> {
        if let Some(request) = self.request(piece.stripe, piece.fragment, &piece.within) {
            let node = request.node.clone();
            match check(&node, self.source.read(request).await, &piece.within) {
                Ok(data) => return Ok(data),
                Err(error) => self.failed(piece.stripe, piece.fragment, &error),
            }
        }
        self.decode(piece).await
    }

    /// The request for bytes `range` of fragment `index` of stripe
    /// `stripe`, unless it failed before or its node did not answer.
    fn request(&self, stripe: usize, index: usize, range: &Range<u64>) -> Option<FragmentRequest> {
        let coded = &self.read.stripes[stripe];
        let location = &coded.fragments()[index];
        if self.bad.contains(&(stripe, index)) || self.unreachable.contains(&location.node) {
            return None;
        }
        let identity = FragmentIdentity {
            shard: self.read.shard.clone(),
            key: self.read.key.clone(),
            version: self.read.version,
            stripe: StripeInfo {
                number: coded.number(),
                count: self.stripe_count,
                offset: coded.offset(),
                data_len: coded.data_len(),
                geometry: coded.geometry(),
                codec: coded.codec(),
            },
            // A stripe has at most 255 fragments.
            index: index as u8,
        };
        Some(FragmentRequest {
            node: location.node.clone(),
            fragment: location.fragment,
            identity,
            range: range.clone(),
        })
    }

    /// Notes that fragment `index` of stripe `stripe` failed with `error`.
    fn failed(&mut self, stripe: usize, index: usize, error: &FragmentReadError) {
        tracing::debug!(key = %self.read.key, stripe, index, %error, "a fragment read failed");
        self.bad.insert((stripe, index));
        if let FragmentReadError::Unreachable { node, .. } = error {
            self.unreachable.insert(node.clone());
        }
    }

    /// Decodes `piece` from the columns covering it of `k` fragments of
    /// its stripe other than those that failed.
    async fn decode(&mut self, piece: &Piece) -> Result<Bytes, CodedReadError> {
        let coded = &self.read.stripes[piece.stripe];
        let (geometry, data_len, number) = (coded.geometry(), coded.data_len(), coded.number());
        let codec = codec_of(coded)?;
        let failed = |source| CodedReadError::Codec {
            stripe: number,
            source,
        };
        let columns = codec
            .columns(geometry, data_len, piece.within.clone())
            .map_err(failed)?;
        let needed = geometry.data_fragments();
        let mut slots: Vec<Option<Bytes>> = vec![None; geometry.total_fragments()];
        let mut readable = 0;
        let mut next = 0;
        loop {
            // Ask as many fragments as are still needed at once, and the
            // next ones in place of any that fail.
            let mut asks = JoinSet::new();
            while asks.len() + readable < needed && next < slots.len() {
                if let Some(request) = self.request(piece.stripe, next, &columns) {
                    let source = Arc::clone(&self.source);
                    asks.spawn(async move {
                        let node = request.node.clone();
                        (next, node, source.read(request).await)
                    });
                }
                next += 1;
            }
            if asks.is_empty() {
                break;
            }
            while let Some(joined) = asks.join_next().await {
                // A fragment read that panicked counts as one that failed.
                let Ok((index, node, result)) = joined else {
                    continue;
                };
                match check(&node, result, &columns) {
                    Ok(data) => {
                        slots[index] = Some(data);
                        readable += 1;
                    }
                    Err(error) => self.failed(piece.stripe, index, &error),
                }
            }
        }
        if readable < needed {
            return Err(CodedReadError::Unreadable {
                stripe: number,
                readable,
                needed,
            });
        }
        let present = seeded::arrange(&slots);
        let data = codec
            .decode_columns(geometry, data_len, columns.clone(), &present)
            .map_err(failed)?;
        // Both bounds lie within `columns`, which lies within a fragment.
        let from = (piece.within.start - columns.start) as usize;
        let to = (piece.within.end - columns.start) as usize;
        Ok(Bytes::copy_from_slice(&data[piece.fragment][from..to]))
    }
}

/// The bytes `node` returned for a fragment read of `range`, checked: as
/// many as asked for, and matching their CRC32C.
fn check(
    node: &NodeId,
    result: Result<FragmentBytes, FragmentReadError>,
    range: &Range<u64>,
) -> Result<Bytes, FragmentReadError> {
    let FragmentBytes { data, crc32c } = result?;
    let damaged = |reason: String| FragmentReadError::Damaged {
        node: node.clone(),
        reason,
    };
    if data.len() as u64 != range.end - range.start {
        return Err(damaged(format!(
            "{} bytes for a range of {range:?}",
            data.len()
        )));
    }
    if !seeded::trusts_crc() && crc32c::crc32c(&data) != crc32c {
        return Err(damaged("the bytes fail their CRC32C".to_owned()));
    }
    Ok(data)
}

/// Seeded bugs of coded reads, for simulations that must catch them.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod seeded {
    use std::cell::Cell;

    use bytes::Bytes;

    /// A bug a coded read can be built with.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadBug {
        /// Bytes that fail their CRC32C are used as they are.
        TrustCrc,
        /// A degraded piece hands the codec the fragments it read in the
        /// first slots, whatever their indices.
        WrongIndices,
    }

    thread_local! {
        static BUG: Cell<Option<ReadBug>> = const { Cell::new(None) };
    }

    /// Seeds `bug` into every coded read on this thread, or removes it.
    pub fn seed_read_bug(bug: Option<ReadBug>) {
        BUG.set(bug);
    }

    pub(super) fn trusts_crc() -> bool {
        BUG.get() == Some(ReadBug::TrustCrc)
    }

    pub(super) fn arrange(slots: &[Option<Bytes>]) -> Vec<Option<&[u8]>> {
        if BUG.get() == Some(ReadBug::WrongIndices) {
            let mut present: Vec<Option<&[u8]>> = slots.iter().flatten().map(|f| Some(&f[..])).collect();
            present.resize(slots.len(), None);
            return present;
        }
        slots.iter().map(|slot| slot.as_deref()).collect()
    }
}

#[cfg(not(feature = "test-util"))]
mod seeded {
    use bytes::Bytes;

    pub(super) fn trusts_crc() -> bool {
        false
    }

    pub(super) fn arrange(slots: &[Option<Bytes>]) -> Vec<Option<&[u8]>> {
        slots.iter().map(|slot| slot.as_deref()).collect()
    }
}
