//! Codec 1: systematic Reed-Solomon over GF(2^16), from `reed-solomon-simd`.

use std::borrow::Cow;
use std::ops::Range;

use reed_solomon_simd::engine::{DefaultEngine, Engine};
use reed_solomon_simd::rate::{HighRateDecoder, HighRateEncoder, RateDecoder, RateEncoder};

use crate::{CodecId, EcCodec, EcError, Geometry};

/// Fragment lengths are multiples of this many bytes.
const FRAGMENT_ALIGN: u64 = 64;

/// Systematic Reed-Solomon, codec ID [`CodecId::REED_SOLOMON_V1`].
///
/// The code is the Leopard-RS construction (FFT-based Reed-Solomon over
/// GF(2^16)) of the `reed-solomon-simd` crate, always in its high-rate
/// form. The crate's default picks the high- or low-rate form from the
/// fragment counts with a heuristic, and the two produce different parity,
/// so this codec names the rate itself rather than inherit a choice a crate
/// upgrade could change. High rate supports every [`Geometry`].
///
/// The layout of a stripe of `L` bytes in a `k+m` geometry:
///
/// - Every fragment is `S` bytes: `⌈L / k⌉` rounded up to a multiple of 64.
///   The 64-byte multiple is the shard size every version of the crate
///   accepts and encodes identically, and the SIMD engines' natural block.
/// - Data fragment `i` (`0 ≤ i < k`) holds bytes `i·S..(i+1)·S` of the
///   data, cut at `L` and padded with zeros to `S` bytes. The last data
///   fragments of a short stripe may be all padding.
/// - Parity fragments `k..k+m` are the crate's recovery shards `0..m` for
///   the `k` data fragments as original shards.
///
/// The code works on each 64-byte column of the fragments alone: bytes
/// `64·j..64·(j+1)` of the parity fragments depend only on the same bytes
/// of the data fragments. A range read decodes just the 64-byte columns
/// that cover its bytes ([`EcCodec::columns`]), which the unit tests and
/// the golden vectors check against whole-stripe decoding.
///
/// The crate picks a SIMD engine (AVX2, SSSE3, Neon, or none) at run time.
/// All of them compute the same bytes; the unit tests check the SIMD path
/// against the portable one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReedSolomonV1;

impl EcCodec for ReedSolomonV1 {
    fn id(&self) -> CodecId {
        CodecId::REED_SOLOMON_V1
    }

    fn fragment_len(&self, geometry: Geometry, data_len: u64) -> Result<u64, EcError> {
        fragment_len(geometry, data_len)
    }

    fn columns(
        &self,
        geometry: Geometry,
        data_len: u64,
        within: Range<u64>,
    ) -> Result<Range<u64>, EcError> {
        let fragment_len = fragment_len(geometry, data_len)?;
        if within.is_empty() || within.end > fragment_len {
            return Err(EcError::OutsideFragment {
                range: within,
                fragment_len,
            });
        }
        // The fragment length is a multiple of the alignment, so rounding
        // the end up stays within it.
        let start = within.start / FRAGMENT_ALIGN * FRAGMENT_ALIGN;
        Ok(start..within.end.next_multiple_of(FRAGMENT_ALIGN))
    }

    fn decode_columns(
        &self,
        geometry: Geometry,
        data_len: u64,
        columns: Range<u64>,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<Vec<u8>>, EcError> {
        decode_columns::<DefaultEngine>(geometry, data_len, columns, fragments)
    }

    fn encode(&self, geometry: Geometry, data: &[u8]) -> Result<Vec<Vec<u8>>, EcError> {
        encode::<DefaultEngine>(geometry, data)
    }

    fn decode(
        &self,
        geometry: Geometry,
        data_len: u64,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<u8>, EcError> {
        decode::<DefaultEngine>(geometry, data_len, fragments)
    }

    fn reconstruct(
        &self,
        geometry: Geometry,
        data_len: u64,
        fragments: &[Option<&[u8]>],
    ) -> Result<Vec<Vec<u8>>, EcError> {
        reconstruct::<DefaultEngine>(geometry, data_len, fragments)
    }
}

/// `⌈data_len / k⌉` rounded up to [`FRAGMENT_ALIGN`]. Also checks that all
/// `k + m` fragments together fit in memory, so callers can compute offsets
/// within the stripe without overflow.
fn fragment_len(geometry: Geometry, data_len: u64) -> Result<u64, EcError> {
    if data_len == 0 {
        return Err(EcError::EmptyStripe);
    }
    let too_large = || EcError::StripeTooLarge { data_len };
    let len = data_len
        .div_ceil(geometry.data_fragments() as u64)
        .checked_next_multiple_of(FRAGMENT_ALIGN)
        .ok_or_else(too_large)?;
    let stripe = len
        .checked_mul(geometry.total_fragments() as u64)
        .ok_or_else(too_large)?;
    usize::try_from(stripe).map_err(|_| too_large())?;
    Ok(len)
}

/// Checks the fragment slots of a stripe and returns the fragment length.
fn check_fragments(
    geometry: Geometry,
    data_len: u64,
    fragments: &[Option<&[u8]>],
) -> Result<usize, EcError> {
    let expected = fragment_len(geometry, data_len)?;
    check_slots(geometry, expected, fragments)?;
    // `fragment_len` checked that the whole stripe fits in a usize.
    Ok(expected as usize)
}

/// Checks that `fragments` has a slot per fragment of `geometry`, that
/// every fragment present has `expected` bytes, and that at least `k` are.
fn check_slots(
    geometry: Geometry,
    expected: u64,
    fragments: &[Option<&[u8]>],
) -> Result<(), EcError> {
    if fragments.len() != geometry.total_fragments() {
        return Err(EcError::FragmentCount {
            expected: geometry.total_fragments(),
            actual: fragments.len(),
        });
    }
    let mut available = 0;
    for (index, fragment) in fragments.iter().enumerate() {
        if let Some(fragment) = fragment {
            if fragment.len() as u64 != expected {
                return Err(EcError::FragmentLength {
                    index,
                    expected,
                    actual: fragment.len(),
                });
            }
            available += 1;
        }
    }
    if available < geometry.data_fragments() {
        return Err(EcError::NotEnoughFragments {
            needed: geometry.data_fragments(),
            available,
        });
    }
    Ok(())
}

fn backend(error: reed_solomon_simd::Error) -> EcError {
    EcError::Backend(error.to_string())
}

fn encode<E: Engine + Default>(geometry: Geometry, data: &[u8]) -> Result<Vec<Vec<u8>>, EcError> {
    // `fragment_len` checked that the whole stripe fits in a usize.
    let len = fragment_len(geometry, data.len() as u64)? as usize;
    let mut fragments: Vec<Vec<u8>> = Vec::with_capacity(geometry.total_fragments());
    for index in 0..geometry.data_fragments() {
        let start = (index * len).min(data.len());
        let end = (start + len).min(data.len());
        let mut fragment = Vec::with_capacity(len);
        fragment.extend_from_slice(&data[start..end]);
        fragment.resize(len, 0);
        fragments.push(fragment);
    }
    let parity = parity::<E>(geometry, len, &fragments)?;
    fragments.extend(parity);
    Ok(fragments)
}

fn decode<E: Engine + Default>(
    geometry: Geometry,
    data_len: u64,
    fragments: &[Option<&[u8]>],
) -> Result<Vec<u8>, EcError> {
    let len = check_fragments(geometry, data_len, fragments)?;
    let data_fragments = restore_data::<E>(geometry, len, fragments)?;
    // The stripe fits in a usize, so its data length does too.
    let mut remaining = data_len as usize;
    let mut data = Vec::with_capacity(remaining);
    for fragment in &data_fragments {
        let take = remaining.min(len);
        data.extend_from_slice(&fragment[..take]);
        remaining -= take;
    }
    Ok(data)
}

fn decode_columns<E: Engine + Default>(
    geometry: Geometry,
    data_len: u64,
    columns: Range<u64>,
    fragments: &[Option<&[u8]>],
) -> Result<Vec<Vec<u8>>, EcError> {
    let fragment_len = fragment_len(geometry, data_len)?;
    let aligned = |at: u64| at.is_multiple_of(FRAGMENT_ALIGN);
    if columns.is_empty()
        || columns.end > fragment_len
        || !aligned(columns.start)
        || !aligned(columns.end)
    {
        return Err(EcError::OutsideFragment {
            range: columns,
            fragment_len,
        });
    }
    let width = columns.end - columns.start;
    check_slots(geometry, width, fragments)?;
    // Within a fragment, which fits in a usize.
    let restored = restore_data::<E>(geometry, width as usize, fragments)?;
    Ok(restored.into_iter().map(Cow::into_owned).collect())
}

fn reconstruct<E: Engine + Default>(
    geometry: Geometry,
    data_len: u64,
    fragments: &[Option<&[u8]>],
) -> Result<Vec<Vec<u8>>, EcError> {
    let len = check_fragments(geometry, data_len, fragments)?;
    let data_fragments = restore_data::<E>(geometry, len, fragments)?;
    let given_parity = &fragments[geometry.data_fragments()..];
    let parity = if given_parity.iter().all(Option::is_some) {
        given_parity.iter().flatten().map(|f| f.to_vec()).collect()
    } else {
        let mut parity = parity::<E>(geometry, len, &data_fragments)?;
        for (rebuilt, given) in parity.iter_mut().zip(given_parity) {
            if let Some(given) = given {
                rebuilt.copy_from_slice(given);
            }
        }
        parity
    };
    let mut all: Vec<Vec<u8>> = data_fragments.into_iter().map(Cow::into_owned).collect();
    all.extend(parity);
    Ok(all)
}

/// The `m` parity fragments of `k` data fragments of `len` bytes each.
fn parity<E: Engine + Default>(
    geometry: Geometry,
    len: usize,
    data_fragments: &[impl AsRef<[u8]>],
) -> Result<Vec<Vec<u8>>, EcError> {
    let mut encoder = HighRateEncoder::<E>::new(
        geometry.data_fragments(),
        geometry.parity_fragments(),
        len,
        E::default(),
        None,
    )
    .map_err(backend)?;
    for fragment in data_fragments {
        encoder.add_original_shard(fragment).map_err(backend)?;
    }
    let result = encoder.encode().map_err(backend)?;
    Ok(result.recovery_iter().map(<[u8]>::to_vec).collect())
}

/// The `k` data fragments of a checked stripe: borrowed where present,
/// decoded where missing.
fn restore_data<'a, E: Engine + Default>(
    geometry: Geometry,
    len: usize,
    fragments: &[Option<&'a [u8]>],
) -> Result<Vec<Cow<'a, [u8]>>, EcError> {
    let (data, parity) = fragments.split_at(geometry.data_fragments());
    let missing = data.iter().filter(|f| f.is_none()).count();
    if missing == 0 {
        return Ok(data.iter().flatten().map(|f| Cow::Borrowed(*f)).collect());
    }

    let mut decoder = HighRateDecoder::<E>::new(
        geometry.data_fragments(),
        geometry.parity_fragments(),
        len,
        E::default(),
        None,
    )
    .map_err(backend)?;
    for (index, fragment) in data.iter().enumerate() {
        if let Some(fragment) = fragment {
            decoder
                .add_original_shard(index, fragment)
                .map_err(backend)?;
        }
    }
    // Exactly `k` fragments in all are enough; extra parity only adds work.
    let present_parity = parity
        .iter()
        .enumerate()
        .filter_map(|(i, f)| Some((i, (*f)?)));
    for (index, fragment) in present_parity.take(missing) {
        decoder
            .add_recovery_shard(index, fragment)
            .map_err(backend)?;
    }
    let result = decoder.decode().map_err(backend)?;
    data.iter()
        .enumerate()
        .map(|(index, fragment)| match fragment {
            Some(fragment) => Ok(Cow::Borrowed(*fragment)),
            None => result
                .restored_original(index)
                .map(|restored| Cow::Owned(restored.to_vec()))
                .ok_or_else(|| EcError::Backend(format!("data fragment {index} not restored"))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use reed_solomon_simd::engine::NoSimd;

    use super::*;

    fn sample(len: usize, seed: u32) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9e37_79b9) | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    /// The run-time SIMD engine and the portable one compute the same
    /// fragments, and each decodes the other's.
    #[test]
    fn simd_and_portable_engines_agree() {
        for (seed, geometry) in Geometry::DESIGN_TABLE.into_iter().enumerate() {
            for data_len in [1, 63, 64, 1000, 4096 * 3 + 7] {
                let data = sample(data_len, seed as u32 + data_len as u32);
                let simd = encode::<DefaultEngine>(geometry, &data).unwrap();
                let portable = encode::<NoSimd>(geometry, &data).unwrap();
                assert_eq!(simd, portable, "{geometry}, {data_len} bytes");

                // Lose the first `m` data fragments, so decoding needs every
                // parity fragment.
                let slots: Vec<Option<&[u8]>> = simd
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (i >= geometry.parity_fragments()).then_some(f.as_slice()))
                    .collect();
                let data_len = data_len as u64;
                assert_eq!(decode::<NoSimd>(geometry, data_len, &slots).unwrap(), data);
                assert_eq!(
                    decode::<DefaultEngine>(geometry, data_len, &slots).unwrap(),
                    data
                );
                assert_eq!(
                    reconstruct::<NoSimd>(geometry, data_len, &slots).unwrap(),
                    simd
                );
            }
        }
    }

    /// Decoding a range of columns of `k` fragments gives the same bytes
    /// as decoding the whole stripe, for every engine.
    #[test]
    fn columns_decode_alone() {
        let codec = ReedSolomonV1;
        for (seed, geometry) in Geometry::DESIGN_TABLE.into_iter().enumerate() {
            let data = sample(4096 * 5 + 77, seed as u32);
            let len = data.len() as u64;
            let fragments = encode::<DefaultEngine>(geometry, &data).unwrap();
            let fragment_len = fragments[0].len() as u64;
            for within in [0..1, 63..65, 100..fragment_len, 0..fragment_len] {
                let columns = codec.columns(geometry, len, within.clone()).unwrap();
                assert!(columns.start <= within.start && within.end <= columns.end);
                let cut = |f: &Vec<u8>| f[columns.start as usize..columns.end as usize].to_vec();
                // Lose the first `m` fragments.
                let cuts: Vec<Vec<u8>> = fragments.iter().map(cut).collect();
                let slots: Vec<Option<&[u8]>> = cuts
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (i >= geometry.parity_fragments()).then_some(f.as_slice()))
                    .collect();
                let expected: Vec<Vec<u8>> = fragments[..geometry.data_fragments()]
                    .iter()
                    .map(cut)
                    .collect();
                for decoded in [
                    decode_columns::<NoSimd>(geometry, len, columns.clone(), &slots).unwrap(),
                    codec
                        .decode_columns(geometry, len, columns.clone(), &slots)
                        .unwrap(),
                ] {
                    assert_eq!(decoded, expected, "{geometry}, columns {columns:?}");
                }
            }
        }
    }

    #[test]
    fn column_ranges_are_checked() {
        let (codec, g) = (ReedSolomonV1, Geometry::RS_4_2);
        // Fragments of 128 bytes.
        assert_eq!(codec.columns(g, 512, 70..71), Ok(64..128));
        assert_eq!(codec.columns(g, 512, 0..128), Ok(0..128));
        for within in [5..5, 0..129] {
            assert_eq!(
                codec.columns(g, 512, within.clone()),
                Err(EcError::OutsideFragment {
                    range: within,
                    fragment_len: 128
                })
            );
        }
        let slots = vec![Some(&[0u8; 64][..]); 6];
        for columns in [0..0, 1..65, 0..100, 64..192] {
            assert_eq!(
                codec.decode_columns(g, 512, columns.clone(), &slots),
                Err(EcError::OutsideFragment {
                    range: columns,
                    fragment_len: 128
                })
            );
        }
        // Each fragment holds the columns' bytes, no more.
        assert_eq!(
            codec.decode_columns(g, 512, 0..128, &slots),
            Err(EcError::FragmentLength {
                index: 0,
                expected: 128,
                actual: 64
            })
        );
        assert!(codec.decode_columns(g, 512, 64..128, &slots).is_ok());
    }

    #[test]
    fn fragment_lengths_are_aligned_and_bounded() {
        let g = Geometry::RS_4_2;
        assert_eq!(fragment_len(g, 0), Err(EcError::EmptyStripe));
        assert_eq!(fragment_len(g, 1), Ok(64));
        assert_eq!(fragment_len(g, 256), Ok(64));
        assert_eq!(fragment_len(g, 257), Ok(128));
        assert_eq!(fragment_len(g, 4 << 20), Ok(1 << 20));
        // Six fragments of a quarter of the data each overflow.
        for data_len in [u64::MAX, u64::MAX / 3 * 2 + 256] {
            assert_eq!(
                fragment_len(g, data_len),
                Err(EcError::StripeTooLarge { data_len })
            );
        }
        // Rounding one fragment up to the alignment overflows.
        let data_len = u64::MAX;
        let mirror = Geometry::new(1, 1).unwrap();
        assert_eq!(
            fragment_len(mirror, data_len),
            Err(EcError::StripeTooLarge { data_len })
        );
    }
}
