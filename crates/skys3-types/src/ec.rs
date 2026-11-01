//! Erasure-coding identifiers (§8.3, §8.4): stripe geometries, codec IDs,
//! fragment IDs and locations, the IDs of the attempts that write
//! fragments, and the coded stripes that `EC_PUBLISH` records hold.
//!
//! They are stored in fragment headers and, from M5-04 on, in the stripe
//! layouts of `EC_PUBLISH` records, so they live here rather than in
//! `skys3-ec`, which implements the codecs and the fragment store and
//! re-exports them.

use std::collections::BTreeMap;
use std::fmt;

use crate::{Epoch, NodeId};

/// The identity of an erasure codec: the code, the fragment layout, and the
/// exact bytes both produce.
///
/// Every stripe records the ID of the codec that encoded it, next to its
/// geometry (design §8.3), and is always decoded with that codec. A codec ID
/// is a persistent format: once a stripe may have been written with it, the
/// codec behind it must keep producing and accepting exactly the same
/// fragments. Golden vectors in `skys3-ec` freeze each one. A change that
/// alters any fragment byte (a new layout, a different code, or a library
/// upgrade that changes output) is a new codec with a new ID, and the old
/// one stays registered so existing stripes still decode.
///
/// ID 0 is never assigned, so a zeroed header field cannot name a codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CodecId(u16);

impl CodecId {
    /// Systematic Reed-Solomon, version 1 (`skys3_ec::ReedSolomonV1`).
    pub const REED_SOLOMON_V1: Self = Self(1);

    /// The codec that encodes new stripes.
    pub const CURRENT: Self = Self::REED_SOLOMON_V1;

    /// The codec ID stored as `raw`, whether or not this build knows it.
    #[must_use]
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    /// The stored form of the ID.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for CodecId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A geometry with no data or no parity fragments, or more than
/// [`Geometry::MAX_FRAGMENTS`] in all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid geometry {data}+{parity}")]
pub struct GeometryError {
    /// The requested number of data fragments.
    pub data: usize,
    /// The requested number of parity fragments.
    pub parity: usize,
}

/// The geometry of a stripe: `k` data fragments and `m` parity fragments
/// (design §8.3), written `k+m`.
///
/// Any `k` of the `k + m` fragments rebuild the stripe, so a stripe survives
/// the loss of any `m` fragments. A stripe records its geometry when it is
/// encoded, and the geometry is never recomputed from the current cluster
/// size (§8.3).
///
/// Fragments are numbered `0..k+m` within the stripe: indexes `0..k` are
/// the data fragments, in data order, and `k..k+m` the parity fragments.
/// The number of a fragment fits in one byte, so a geometry has at most
/// [`Geometry::MAX_FRAGMENTS`] fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Geometry {
    data: u8,
    parity: u8,
}

impl Geometry {
    /// The most fragments (`k + m`) a stripe can have.
    pub const MAX_FRAGMENTS: usize = u8::MAX as usize;

    /// 3+2, for 5 or 6 eligible nodes (§8.3).
    pub const RS_3_2: Self = Self { data: 3, parity: 2 };
    /// 4+2, for 7 or 8 eligible nodes (§8.3).
    pub const RS_4_2: Self = Self { data: 4, parity: 2 };
    /// 6+2, for 9 or 10 eligible nodes (§8.3).
    pub const RS_6_2: Self = Self { data: 6, parity: 2 };
    /// 8+2, for 11 or more eligible nodes (§8.3).
    pub const RS_8_2: Self = Self { data: 8, parity: 2 };

    /// The geometries of the §8.3 table, narrowest first.
    pub const DESIGN_TABLE: [Self; 4] = [Self::RS_3_2, Self::RS_4_2, Self::RS_6_2, Self::RS_8_2];

    /// A geometry of `data` data fragments and `parity` parity fragments.
    ///
    /// # Errors
    ///
    /// [`GeometryError`] if either count is zero or the stripe would have
    /// more than [`Geometry::MAX_FRAGMENTS`] fragments.
    pub fn new(data: usize, parity: usize) -> Result<Self, GeometryError> {
        let invalid = GeometryError { data, parity };
        if data == 0 || parity == 0 || data.saturating_add(parity) > Self::MAX_FRAGMENTS {
            return Err(invalid);
        }
        Ok(Self {
            data: u8::try_from(data).map_err(|_| invalid)?,
            parity: u8::try_from(parity).map_err(|_| invalid)?,
        })
    }

    /// The number of data fragments, `k`.
    #[must_use]
    pub fn data_fragments(self) -> usize {
        usize::from(self.data)
    }

    /// The number of parity fragments, `m`: how many fragments the stripe
    /// can lose.
    #[must_use]
    pub fn parity_fragments(self) -> usize {
        usize::from(self.parity)
    }

    /// The number of fragments in the stripe, `k + m`.
    #[must_use]
    pub fn total_fragments(self) -> usize {
        self.data_fragments() + self.parity_fragments()
    }

    /// Whether fragment `index` holds data rather than parity.
    #[must_use]
    pub fn is_data_fragment(self, index: usize) -> bool {
        index < self.data_fragments()
    }
}

impl fmt::Display for Geometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}+{}", self.data, self.parity)
    }
}

/// The ID of a fragment on the node that stores it (§8.4).
///
/// A node's fragment store assigns the ID when it makes the fragment
/// durable and acknowledges it, and the stripe layout names the fragment by
/// its node and this ID. Compaction of fragment segments moves a fragment
/// within its node without changing its ID (§10.3), so the ID is not a
/// location. A node never assigns an acknowledged ID twice: the store
/// derives it from the disk and the place where the fragment was first
/// written (`skys3_ec::FragmentStore`).
///
/// The text form is 32 lowercase hexadecimal digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentId(u128);

impl FragmentId {
    /// The fragment ID stored as `raw`.
    #[must_use]
    pub const fn new(raw: u128) -> Self {
        Self(raw)
    }

    /// The stored form of the ID.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

impl fmt::Display for FragmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// The ID of an attempt that writes fragments: an encoding (M5-04), a
/// repair (M5-08), or a move (M5-09).
///
/// Every fragment header names the attempt that wrote it, so a shard's
/// primary can tell fragments of an attempt still in progress from orphans
/// of an abandoned one before it lets a node reclaim them (§8.4). An attempt
/// belongs to one shard, which the fragment header also names. It is
/// identified by the epoch in which the shard's primary started it and a
/// number that primary never uses for another attempt in that epoch; how
/// the number is drawn, including across a restart of the primary in the
/// same epoch, is the encoder's choice (M5-04).
///
/// Attempts order by epoch, then number. The text form is
/// `<epoch>/<number>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptId {
    /// The epoch of the primary that started the attempt.
    pub epoch: Epoch,
    /// The attempt's number within that epoch.
    pub number: u64,
}

impl AttemptId {
    /// Pairs an epoch and a number.
    #[must_use]
    pub const fn new(epoch: Epoch, number: u64) -> Self {
        Self { epoch, number }
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.epoch, self.number)
    }
}

/// Where a fragment is: the node that stores it, and its ID in that node's
/// fragment store.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentLocation {
    /// The node that stores the fragment.
    pub node: NodeId,
    /// The fragment's ID in that node's store.
    pub fragment: FragmentId,
}

/// Why a [`CodedStripe`] could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CodedStripeError {
    /// The stripe holds no data: a stripe holds at least one byte (§8.4).
    #[error("stripe {stripe} holds no data")]
    Empty {
        /// The stripe's number.
        stripe: u32,
    },
    /// The stripe ends past the largest object offset.
    #[error("stripe {stripe} ends past the largest object offset")]
    PastEnd {
        /// The stripe's number.
        stripe: u32,
    },
    /// The stripe does not locate exactly one fragment per index.
    #[error("stripe {stripe} is {geometry} but locates {located} fragments")]
    FragmentCount {
        /// The stripe's number.
        stripe: u32,
        /// The stripe's geometry.
        geometry: Geometry,
        /// The fragments it locates.
        located: usize,
    },
    /// Two fragments of the stripe are on one node: losing that node would
    /// lose both (§6.7).
    #[error("stripe {stripe} puts fragments {first} and {second} on node {node}")]
    SharedNode {
        /// The stripe's number.
        stripe: u32,
        /// The node.
        node: NodeId,
        /// The lower fragment index on it.
        first: usize,
        /// The higher fragment index on it.
        second: usize,
    },
}

/// One coded stripe, as the stripe layouts of an `EC_PUBLISH` record hold
/// it (§8.3, §8.4): where the stripe's data sits in the object, its
/// geometry and codec, and the location of each of its `k + m` fragments.
///
/// The geometry, the codec, and the locations are fixed when the stripe is
/// encoded and are never recomputed from the cluster as it is later:
/// growing or shrinking the cluster changes only new stripes. Repair and
/// moves change a location only by committing a record that names the new
/// one (`EC_RELOCATE`, plan M5-08 and M5-09).
///
/// A value always locates exactly one fragment per index and never two on
/// one node. Whether two nodes share a failure domain needs their labels,
/// so the per-domain cap is the placement's to keep
/// (`skys3_coord::FragmentPlanner`).
///
/// ```
/// use skys3_types::{CodecId, CodedStripe, FragmentId, FragmentLocation, Geometry, NodeId};
///
/// let fragments = (0..6)
///     .map(|i| FragmentLocation {
///         node: NodeId::new(format!("node-{i}")).unwrap(),
///         fragment: FragmentId::new(i),
///     })
///     .collect();
/// let stripe = CodedStripe::new(0, 0, 1 << 20, Geometry::RS_4_2, CodecId::CURRENT, fragments)?;
/// assert_eq!(stripe.fragments().len(), 6);
/// assert_eq!(stripe.end(), 1 << 20);
/// # Ok::<(), skys3_types::CodedStripeError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodedStripe {
    number: u32,
    offset: u64,
    data_len: u64,
    geometry: Geometry,
    codec: CodecId,
    fragments: Vec<FragmentLocation>,
}

impl CodedStripe {
    /// Stripe `number` of an object: `data_len` bytes from `offset`, coded
    /// as `geometry` by `codec`, with fragment `i` at `fragments[i]`.
    ///
    /// # Errors
    ///
    /// [`CodedStripeError`] if the stripe holds no data, ends past
    /// `u64::MAX`, does not locate `k + m` fragments, or puts two on one
    /// node.
    pub fn new(
        number: u32,
        offset: u64,
        data_len: u64,
        geometry: Geometry,
        codec: CodecId,
        fragments: Vec<FragmentLocation>,
    ) -> Result<Self, CodedStripeError> {
        if data_len == 0 {
            return Err(CodedStripeError::Empty { stripe: number });
        }
        if offset.checked_add(data_len).is_none() {
            return Err(CodedStripeError::PastEnd { stripe: number });
        }
        if fragments.len() != geometry.total_fragments() {
            return Err(CodedStripeError::FragmentCount {
                stripe: number,
                geometry,
                located: fragments.len(),
            });
        }
        let mut seen: BTreeMap<&NodeId, usize> = BTreeMap::new();
        for (index, location) in fragments.iter().enumerate() {
            if let Some(first) = seen.insert(&location.node, index) {
                return Err(CodedStripeError::SharedNode {
                    stripe: number,
                    node: location.node.clone(),
                    first,
                    second: index,
                });
            }
        }
        Ok(Self {
            number,
            offset,
            data_len,
            geometry,
            codec,
            fragments,
        })
    }

    /// The stripe's number within the object.
    #[must_use]
    pub fn number(&self) -> u32 {
        self.number
    }

    /// Where the stripe's data starts in the object.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The stripe's data length.
    #[must_use]
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    /// Where the stripe's data ends in the object, exclusive.
    #[must_use]
    pub fn end(&self) -> u64 {
        // `new` checked that this does not overflow.
        self.offset + self.data_len
    }

    /// The stripe's geometry, as encoded.
    #[must_use]
    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    /// The codec that decodes the stripe.
    #[must_use]
    pub fn codec(&self) -> CodecId {
        self.codec
    }

    /// Each fragment's location, by fragment index: the `k` data fragments,
    /// then the `m` parity fragments.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentLocation] {
        &self.fragments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_design_geometries() {
        for geometry in Geometry::DESIGN_TABLE {
            let rebuilt = Geometry::new(geometry.data_fragments(), geometry.parity_fragments());
            assert_eq!(rebuilt, Ok(geometry));
        }
        assert_eq!(Geometry::RS_4_2.to_string(), "4+2");
        assert_eq!(Geometry::RS_8_2.total_fragments(), 10);
        assert!(Geometry::RS_3_2.is_data_fragment(2));
        assert!(!Geometry::RS_3_2.is_data_fragment(3));
    }

    #[test]
    fn rejects_degenerate_and_oversized_geometries() {
        for (data, parity) in [(0, 2), (4, 0), (0, 0), (254, 2), (256, 1), (1, usize::MAX)] {
            let error = GeometryError { data, parity };
            assert_eq!(Geometry::new(data, parity), Err(error));
            assert_eq!(
                error.to_string(),
                format!("invalid geometry {data}+{parity}")
            );
        }
        let widest = Geometry::new(253, 2).unwrap();
        assert_eq!(widest.total_fragments(), Geometry::MAX_FRAGMENTS);
    }

    fn located(nodes: &[usize]) -> Vec<FragmentLocation> {
        nodes
            .iter()
            .zip(0..)
            .map(|(n, i)| FragmentLocation {
                node: NodeId::new(format!("node-{n}")).unwrap(),
                fragment: FragmentId::new(i),
            })
            .collect()
    }

    #[test]
    fn coded_stripes_locate_one_fragment_per_index_and_node() {
        let codec = CodecId::CURRENT;
        let fragments = located(&[0, 1, 2, 3, 4]);
        let stripe =
            CodedStripe::new(2, 128, 64, Geometry::RS_3_2, codec, fragments.clone()).unwrap();
        assert_eq!(
            (stripe.number(), stripe.offset(), stripe.data_len()),
            (2, 128, 64)
        );
        assert_eq!(stripe.end(), 192);
        assert_eq!(
            (stripe.geometry(), stripe.codec()),
            (Geometry::RS_3_2, codec)
        );
        assert_eq!(stripe.fragments(), fragments.as_slice());

        let build = |offset, len, nodes: &[usize]| {
            CodedStripe::new(7, offset, len, Geometry::RS_3_2, codec, located(nodes))
        };
        let cases = [
            (build(0, 0, &[0, 1, 2, 3, 4]), "stripe 7 holds no data"),
            (
                build(u64::MAX, 1, &[0, 1, 2, 3, 4]),
                "stripe 7 ends past the largest object offset",
            ),
            (
                build(0, 1, &[0, 1, 2, 3]),
                "stripe 7 is 3+2 but locates 4 fragments",
            ),
            (
                build(0, 1, &[0, 1, 2, 3, 4, 5]),
                "stripe 7 is 3+2 but locates 6 fragments",
            ),
            (
                build(0, 1, &[0, 1, 2, 1, 4]),
                "stripe 7 puts fragments 1 and 3 on node node-1",
            ),
        ];
        for (result, message) in cases {
            assert_eq!(result.unwrap_err().to_string(), message);
        }
    }

    #[test]
    fn ids_print_and_order() {
        assert_eq!(CodecId::REED_SOLOMON_V1.to_string(), "1");
        assert_eq!(CodecId::new(7).get(), 7);
        assert_eq!(CodecId::CURRENT, CodecId::REED_SOLOMON_V1);
        let id = FragmentId::new(0x2a << 64 | 0x1000);
        assert_eq!(id.get(), 0x2a << 64 | 0x1000);
        assert_eq!(id.to_string(), "000000000000002a0000000000001000");
        let early = AttemptId::new(Epoch::new(3), 9);
        let late = AttemptId::new(Epoch::new(4), 1);
        assert!(early < late);
        assert!(early < AttemptId::new(Epoch::new(3), 10));
        assert_eq!(early.to_string(), "3/9");
    }
}
