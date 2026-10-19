//! The versioned codec interface and the registry of known codecs.

use std::fmt;
use std::ops::Range;

use crate::{CodecId, EcError, Geometry, ReedSolomonV1};

/// An erasure codec: splits a stripe's data into `k` data fragments, adds
/// `m` parity fragments, and rebuilds the data or any lost fragment from any
/// `k` of them (design §8.4).
///
/// Codecs are systematic: data fragment `i` is a plain copy of a contiguous
/// range of the stripe's data ([`EcCodec::data_range`]), so a healthy read
/// fetches only the data fragments covering the range it needs, with no
/// decoding (§8.5).
///
/// Every fragment of a stripe has the same length,
/// [`EcCodec::fragment_len`], which depends only on the codec, the geometry,
/// and the stripe's data length. Those three values, recorded with every
/// stripe, are all a reader needs besides `k` fragments.
///
/// Fragments are passed by index: a slice of `k + m` slots in which slot `i`
/// holds fragment `i`, or `None` if it is missing. The codec checks the
/// slot count, every fragment's length, and that at least `k` are present
/// before it decodes, and reports a mismatch as an [`EcError`]. It cannot
/// tell a corrupt fragment of the right length from a good one: callers
/// verify each fragment's checksum first and pass a corrupt fragment as
/// missing.
pub trait EcCodec: fmt::Debug + Send + Sync {
    /// The ID stored with every stripe this codec encodes.
    fn id(&self) -> CodecId;

    /// The length of each fragment of a stripe of `data_len` bytes.
    ///
    /// # Errors
    ///
    /// [`EcError::EmptyStripe`] if `data_len` is zero, and
    /// [`EcError::StripeTooLarge`] if the fragments would not fit in memory.
    fn fragment_len(&self, geometry: Geometry, data_len: u64) -> Result<u64, EcError>;

    /// The range of the stripe's data that data fragment `index` holds.
    ///
    /// The range is empty for a data fragment that holds only padding, as
    /// the last data fragments of a short stripe may. The provided method
    /// describes the contiguous layout: fragment `i` holds bytes
    /// `i * len..(i + 1) * len` of the data, where `len` is the fragment
    /// length, cut at the data length and padded with zeros. A codec with a
    /// different layout overrides it.
    ///
    /// # Errors
    ///
    /// [`EcError::NotADataFragment`] if `index` is not below `k`, and the
    /// errors of [`EcCodec::fragment_len`].
    fn data_range(
        &self,
        geometry: Geometry,
        data_len: u64,
        index: usize,
    ) -> Result<Range<u64>, EcError> {
        if !geometry.is_data_fragment(index) {
            return Err(EcError::NotADataFragment {
                index,
                data_fragments: geometry.data_fragments(),
            });
        }
        let fragment_len = self.fragment_len(geometry, data_len)?;
        // `index < k`, and `k * fragment_len` fits in a u64 (it is checked
        // by `fragment_len`), so neither product overflows.
        let index = index as u64;
        let start = (index * fragment_len).min(data_len);
        let end = ((index + 1) * fragment_len).min(data_len);
        Ok(start..end)
    }

    /// The bytes of every fragment that decoding bytes `within` of a data
    /// fragment needs: a range read that lost a data fragment reads these
    /// bytes of `k` other fragments and decodes only them (§8.5,
    /// [`EcCodec::decode_columns`]). The range contains `within`, and is
    /// the same in every fragment of the stripe.
    ///
    /// # Errors
    ///
    /// [`EcError::OutsideFragment`] if `within` is empty or ends past the
    /// fragment length, and the errors of [`EcCodec::fragment_len`].
    fn columns(
        &self,
        geometry: Geometry,
        data_len: u64,
        within: Range<u64>,
    ) -> Result<Range<u64>, EcError>;

    /// Rebuilds bytes `columns` of every data fragment of a stripe from the
    /// same bytes of any `k` of its fragments: `fragments` holds, for each
    /// fragment present, its bytes `columns`, where `columns` is a range
    /// [`EcCodec::columns`] returned. Returns the `k` data fragments' bytes
    /// `columns`, in index order.
    ///
    /// # Errors
    ///
    /// [`EcError::OutsideFragment`] if `columns` is not such a range, and
    /// otherwise as [`EcCodec::decode`], with fragment lengths checked
    /// against the length of `columns`.
    fn decode_columns(
        &self,
        geometry: Geometry,
        data_len: u64,
        columns: Range<u64>,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<Vec<u8>>, EcError>;

    /// Encodes one stripe: returns its `k + m` fragments, in index order.
    ///
    /// # Errors
    ///
    /// [`EcError::EmptyStripe`] if `data` is empty.
    fn encode(&self, geometry: Geometry, data: &[u8]) -> Result<Vec<Vec<u8>>, EcError>;

    /// Rebuilds a stripe's `data_len` bytes of data from any `k` of its
    /// fragments.
    ///
    /// When every data fragment is present this only copies them; otherwise
    /// it decodes the missing ones.
    ///
    /// # Errors
    ///
    /// [`EcError::FragmentCount`], [`EcError::FragmentLength`], or
    /// [`EcError::NotEnoughFragments`] if `fragments` does not fit the
    /// stripe, and the errors of [`EcCodec::fragment_len`].
    fn decode(
        &self,
        geometry: Geometry,
        data_len: u64,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<u8>, EcError>;

    /// Rebuilds every fragment of a stripe, data and parity, from any `k` of
    /// them: repair rebuilds a lost fragment with it (§8.6).
    ///
    /// Present fragments are returned unchanged, so the result equals what
    /// [`EcCodec::encode`] returned for the stripe as long as the present
    /// fragments are intact.
    ///
    /// # Errors
    ///
    /// The errors of [`EcCodec::decode`].
    fn reconstruct(
        &self,
        geometry: Geometry,
        data_len: u64,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<Vec<u8>>, EcError>;
}

/// The codec a stripe names, for decoding or repairing it.
///
/// # Errors
///
/// [`EcError::UnknownCodec`] if this build does not know `id`, for example a
/// stripe written by a newer release.
pub fn codec(id: CodecId) -> Result<&'static dyn EcCodec, EcError> {
    match id {
        CodecId::REED_SOLOMON_V1 => Ok(&ReedSolomonV1),
        _ => Err(EcError::UnknownCodec(id)),
    }
}

/// The codec that encodes new stripes, [`CodecId::CURRENT`].
#[must_use]
pub fn current_codec() -> &'static dyn EcCodec {
    &ReedSolomonV1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_resolves_known_ids_only() {
        assert_eq!(
            codec(CodecId::REED_SOLOMON_V1).unwrap().id(),
            CodecId::REED_SOLOMON_V1
        );
        assert_eq!(current_codec().id(), CodecId::CURRENT);
        for raw in [0, 2, u16::MAX] {
            let id = CodecId::new(raw);
            assert_eq!(codec(id).unwrap_err(), EcError::UnknownCodec(id));
        }
    }
}
