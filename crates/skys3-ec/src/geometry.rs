//! Stripe geometry: how many data and parity fragments a stripe has.

use std::fmt;

use crate::EcError;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// [`EcError::InvalidGeometry`] if either count is zero or the stripe
    /// would have more than [`Geometry::MAX_FRAGMENTS`] fragments.
    pub fn new(data: usize, parity: usize) -> Result<Self, EcError> {
        let invalid = || EcError::InvalidGeometry { data, parity };
        if data == 0 || parity == 0 || data.saturating_add(parity) > Self::MAX_FRAGMENTS {
            return Err(invalid());
        }
        Ok(Self {
            data: u8::try_from(data).map_err(|_| invalid())?,
            parity: u8::try_from(parity).map_err(|_| invalid())?,
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
            assert_eq!(
                Geometry::new(data, parity),
                Err(EcError::InvalidGeometry { data, parity })
            );
        }
        let widest = Geometry::new(253, 2).unwrap();
        assert_eq!(widest.total_fragments(), Geometry::MAX_FRAGMENTS);
    }
}
