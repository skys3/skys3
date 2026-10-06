//! Errors of the erasure-coding layer.

use skys3_types::{CodecId, GeometryError};

/// Why an erasure-coding operation failed.
///
/// Every check runs before any coding work, so malformed or mismatched
/// fragments (wrong count, wrong length, too few) are reported here and
/// never cause a panic.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EcError {
    /// A geometry with no data or no parity fragments, or more than
    /// [`Geometry::MAX_FRAGMENTS`](crate::Geometry::MAX_FRAGMENTS) in all.
    #[error("invalid geometry {data}+{parity}")]
    InvalidGeometry {
        /// The requested number of data fragments.
        data: usize,
        /// The requested number of parity fragments.
        parity: usize,
    },

    /// A stripe names a codec this build does not know.
    #[error("unknown erasure codec {0}")]
    UnknownCodec(CodecId),

    /// A stripe must hold at least one byte of data.
    #[error("a stripe must hold at least one byte of data")]
    EmptyStripe,

    /// The stripe's data length does not fit in memory on this platform.
    #[error("a stripe of {data_len} bytes is too large")]
    StripeTooLarge {
        /// The stripe's data length.
        data_len: u64,
    },

    /// The caller passed a fragment list whose length is not `k + m`.
    #[error("expected {expected} fragment slots, got {actual}")]
    FragmentCount {
        /// `k + m`.
        expected: usize,
        /// The number of slots passed.
        actual: usize,
    },

    /// A fragment's length does not match the stripe's geometry and data
    /// length.
    #[error("fragment {index} has {actual} bytes, expected {expected}")]
    FragmentLength {
        /// The fragment's index within the stripe.
        index: usize,
        /// The fragment length the stripe's codec, geometry, and data length
        /// call for.
        expected: u64,
        /// The fragment's actual length.
        actual: usize,
    },

    /// A data-fragment operation named a parity fragment or an index past
    /// the stripe.
    #[error("fragment {index} is not one of the {data_fragments} data fragments")]
    NotADataFragment {
        /// The index passed.
        index: usize,
        /// `k`.
        data_fragments: usize,
    },

    /// Fewer than `k` fragments are available.
    #[error("{available} fragments available, at least {needed} needed")]
    NotEnoughFragments {
        /// `k`.
        needed: usize,
        /// The number of fragments passed.
        available: usize,
    },

    /// The Reed-Solomon library rejected an operation that the checks above
    /// let through. This indicates a bug, not bad input.
    #[error("reed-solomon backend error: {0}")]
    Backend(String),
}

impl From<GeometryError> for EcError {
    fn from(error: GeometryError) -> Self {
        Self::InvalidGeometry {
            data: error.data,
            parity: error.parity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Geometry;

    #[test]
    fn an_invalid_geometry_converts() {
        let error = EcError::from(Geometry::new(0, 2).unwrap_err());
        assert_eq!(error, EcError::InvalidGeometry { data: 0, parity: 2 });
    }
}
