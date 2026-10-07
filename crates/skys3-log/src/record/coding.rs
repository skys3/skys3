//! The body of `EC_PUBLISH`, which publishes an object version's
//! erasure-coded layout (§8.4).
//!
//! The record names the version it encodes by the position of the record
//! that committed it and its ETag, the attempt that wrote the fragments,
//! and the version's size, which its stripes cover exactly once, in order.
//! A stripe is encoded as its data length, its geometry (`k` and `m`, a
//! `u8` each), its codec ID (`u16`), and then, for each of its `k + m`
//! fragments in index order, the node that holds it (a `u8` length and
//! the node ID) and its fragment ID (`u128`, little-endian). A stripe's
//! number and offset are its place in the list and the sum of the lengths
//! before it, so they take no bytes.

use skys3_types::{
    AttemptId, CodecId, CodedStripe, ETag, Epoch, EpochSeq, FragmentId, FragmentLocation, Geometry,
    NodeId,
};

use super::MAX_KEY_LEN;
use super::body::{check_precedes, invalid, read_etag, write_etag};
use super::error::{FieldError, Problem};
use super::wire::{Reader, Writer};

/// The most stripes an `EC_PUBLISH` lists. The 2 MiB header bound binds
/// first for all but the narrowest stripes: an object that needs more than
/// fit stays replicated.
pub const MAX_STRIPES: usize = 1 << 16;

/// The fewest bytes an encoded stripe takes: its length, geometry, and
/// codec, and two fragments with one-byte node IDs.
const MIN_STRIPE_LEN: usize = 8 + 1 + 1 + 2 + 2 * (1 + 1 + 16);

/// Publishes the coded layout of a version of `key`: `EC_PUBLISH` (§8.4).
///
/// The primary commits it once every fragment of every stripe is durable.
/// Applying it marks the version coded, after which the members drop their
/// replicas; a record whose version is no longer the key's current one is
/// dropped when applied, and its fragments become orphans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcPublish {
    /// The object key.
    pub key: String,
    /// The position of the record that committed the version encoded,
    /// which precedes this record.
    pub version: EpochSeq,
    /// The version's ETag. With `version`, it is the version identity the
    /// fragment headers carry.
    pub etag: ETag,
    /// The attempt that wrote the fragments.
    pub attempt: AttemptId,
    /// The version's size: the sum of the stripes' data lengths.
    pub size: u64,
    /// The stripes, numbered from 0, each starting where the one before
    /// ends.
    pub stripes: Vec<CodedStripe>,
}

impl EcPublish {
    pub(super) fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("ec_publish.key", &self.key, 1, MAX_KEY_LEN)?;
        check_precedes("ec_publish.version", self.version, position)?;
        w.position(self.version);
        write_etag(w, "ec_publish.etag", &self.etag)?;
        w.u64(self.attempt.epoch.get());
        w.u64(self.attempt.number);
        w.u64(self.size);
        w.len32("ec_publish.stripes", self.stripes.len(), 1, MAX_STRIPES)?;
        let mut offset = 0;
        for (number, stripe) in self.stripes.iter().enumerate() {
            if stripe.number() as usize != number || stripe.offset() != offset {
                return Err(FieldError::new(
                    "ec_publish.stripes",
                    Problem::Inconsistent("stripes are not numbered and placed in order"),
                ));
            }
            offset = stripe.end();
            write_stripe(w, stripe)?;
        }
        check_size(self.size, offset)
    }

    pub(super) fn decode(r: &mut Reader<'_>, position: EpochSeq) -> Result<Self, FieldError> {
        let key = r.str16("ec_publish.key", 1, MAX_KEY_LEN)?;
        let version = r.position("ec_publish.version")?;
        check_precedes("ec_publish.version", version, position)?;
        let etag = read_etag(r, "ec_publish.etag")?;
        let attempt = AttemptId::new(
            Epoch::new(r.u64("ec_publish.attempt")?),
            r.u64("ec_publish.attempt")?,
        );
        let size = r.u64("ec_publish.size")?;
        let count = r.len32("ec_publish.stripes", 1, MAX_STRIPES)?;
        r.check_count("ec_publish.stripes", count, MIN_STRIPE_LEN)?;
        let mut stripes = Vec::with_capacity(count);
        let mut offset = 0u64;
        for number in 0..count {
            // `count` is at most `MAX_STRIPES`, which fits a u32.
            let stripe = read_stripe(r, number as u32, offset)?;
            offset = stripe.end();
            stripes.push(stripe);
        }
        check_size(size, offset)?;
        Ok(Self {
            key,
            version,
            etag,
            attempt,
            size,
            stripes,
        })
    }
}

fn check_size(size: u64, covered: u64) -> Result<(), FieldError> {
    if size == covered {
        Ok(())
    } else {
        Err(FieldError::new(
            "ec_publish.size",
            Problem::Inconsistent("the stripes do not cover the object exactly"),
        ))
    }
}

fn write_stripe(w: &mut Writer<'_>, stripe: &CodedStripe) -> Result<(), FieldError> {
    if stripe.codec().get() == 0 {
        return Err(invalid("ec_publish.codec", "codec ID 0 is never assigned"));
    }
    w.u64(stripe.data_len());
    let geometry = stripe.geometry();
    // A geometry has at most 255 fragments, so each count fits a byte.
    w.u8(geometry.data_fragments() as u8);
    w.u8(geometry.parity_fragments() as u8);
    w.u16(stripe.codec().get());
    for location in stripe.fragments() {
        w.str8(
            "ec_publish.node",
            location.node.as_str(),
            1,
            NodeId::MAX_LEN,
        )?;
        w.raw(&location.fragment.get().to_le_bytes());
    }
    Ok(())
}

fn read_stripe(r: &mut Reader<'_>, number: u32, offset: u64) -> Result<CodedStripe, FieldError> {
    let data_len = r.u64("ec_publish.data_len")?;
    let (k, m) = (r.u8("ec_publish.geometry")?, r.u8("ec_publish.geometry")?);
    let geometry =
        Geometry::new(k.into(), m.into()).map_err(|error| invalid("ec_publish.geometry", error))?;
    let codec = CodecId::new(r.u16("ec_publish.codec")?);
    if codec.get() == 0 {
        return Err(invalid("ec_publish.codec", "codec ID 0 is never assigned"));
    }
    let total = geometry.total_fragments();
    r.check_count("ec_publish.fragments", total, 1 + 1 + 16)?;
    let mut fragments = Vec::with_capacity(total);
    for _ in 0..total {
        let node = r.str8("ec_publish.node", 1, NodeId::MAX_LEN)?;
        let node = NodeId::new(node).map_err(|error| invalid("ec_publish.node", error))?;
        let mut id = [0; 16];
        id.copy_from_slice(r.take("ec_publish.fragment", 16)?);
        fragments.push(FragmentLocation {
            node,
            fragment: FragmentId::new(u128::from_le_bytes(id)),
        });
    }
    CodedStripe::new(number, offset, data_len, geometry, codec, fragments)
        .map_err(|error| invalid("ec_publish.stripes", error))
}
