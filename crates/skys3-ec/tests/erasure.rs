//! Every erasure pattern of up to `m` fragments decodes (plan M5-01).
//!
//! The exhaustive tests try every subset of lost fragments, not a sample,
//! for every geometry of the design's table (§8.3) and for every geometry up
//! to 8+4, which covers the configurable `max_data_fragments` and
//! `parity_fragments` defaults and the design example's 6+3. A proptest
//! covers data sizes.

use proptest::prelude::*;
use skys3_ec::{CodecId, EcCodec, EcError, Geometry, codec, current_codec};

fn rs() -> &'static dyn EcCodec {
    codec(CodecId::REED_SOLOMON_V1).unwrap()
}

/// Deterministic, incompressible test data.
fn sample(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// The fragments with those whose bit is set in `lost` removed.
fn without(fragments: &[Vec<u8>], lost: u32) -> Vec<Option<&[u8]>> {
    fragments
        .iter()
        .enumerate()
        .map(|(i, f)| (lost & (1 << i) == 0).then_some(f.as_slice()))
        .collect()
}

/// Encodes `data`, then decodes and reconstructs it after every erasure
/// pattern of up to `m` fragments, and checks that every pattern of `m + 1`
/// fails cleanly. Returns the number of patterns that decoded.
fn check_every_pattern(geometry: Geometry, data: &[u8]) -> usize {
    let codec = rs();
    let data_len = data.len() as u64;
    let fragments = codec.encode(geometry, data).unwrap();
    assert_eq!(fragments.len(), geometry.total_fragments());

    let total = geometry.total_fragments();
    let mut decoded = 0;
    for lost in 0u32..(1 << total) {
        let erased = lost.count_ones() as usize;
        if erased > geometry.parity_fragments() + 1 {
            continue;
        }
        let slots = without(&fragments, lost);
        if erased <= geometry.parity_fragments() {
            let context = format!("{geometry}, {data_len} bytes, lost {lost:#b}");
            assert_eq!(
                codec.decode(geometry, data_len, &slots).unwrap(),
                data,
                "{context}"
            );
            assert_eq!(
                codec.reconstruct(geometry, data_len, &slots).unwrap(),
                fragments,
                "{context}"
            );
            decoded += 1;
        } else {
            let expected = EcError::NotEnoughFragments {
                needed: geometry.data_fragments(),
                available: total - erased,
            };
            assert_eq!(
                codec.decode(geometry, data_len, &slots),
                Err(expected.clone())
            );
            assert_eq!(codec.reconstruct(geometry, data_len, &slots), Err(expected));
        }
    }
    decoded
}

/// `C(n, 0) + C(n, 1) + ... + C(n, r)`.
fn patterns_up_to(n: usize, r: usize) -> usize {
    let mut sum = 0;
    let mut choose = 1;
    for i in 0..=r {
        sum += choose;
        choose = choose * (n - i) / (i + 1);
    }
    sum
}

#[test]
fn every_erasure_pattern_decodes_for_every_design_geometry() {
    for geometry in Geometry::DESIGN_TABLE {
        let k = geometry.data_fragments();
        // One byte; a stripe whose last data fragments are all padding; a
        // stripe that fills its fragments exactly; and one that spans
        // several 64-byte blocks per fragment and ends mid-fragment.
        for (seed, data_len) in [1, 65, 64 * k, 64 * k * 5 + 17].into_iter().enumerate() {
            let data = sample(data_len, seed as u64);
            let decoded = check_every_pattern(geometry, &data);
            let expected = patterns_up_to(geometry.total_fragments(), geometry.parity_fragments());
            assert_eq!(decoded, expected, "{geometry}");
        }
    }
}

#[test]
fn every_erasure_pattern_decodes_for_every_geometry_up_to_8_plus_4() {
    for data_fragments in 1..=8 {
        for parity_fragments in 1..=4 {
            let geometry = Geometry::new(data_fragments, parity_fragments).unwrap();
            let data = sample(64 * data_fragments * 2 - 3, data_fragments as u64);
            let decoded = check_every_pattern(geometry, &data);
            let expected = patterns_up_to(geometry.total_fragments(), parity_fragments);
            assert_eq!(decoded, expected, "{geometry}");
        }
    }
}

#[test]
fn data_fragments_hold_the_data_in_order() {
    let codec = rs();
    let geometry = Geometry::RS_4_2;
    let data = sample(1000, 7);
    let fragments = codec.encode(geometry, &data).unwrap();
    // 1000 bytes in 4 fragments: 250 each, rounded up to 256.
    assert_eq!(codec.fragment_len(geometry, 1000), Ok(256));
    assert!(fragments.iter().all(|f| f.len() == 256));
    let ranges: Vec<_> = (0..4)
        .map(|i| codec.data_range(geometry, 1000, i).unwrap())
        .collect();
    assert_eq!(ranges, [0..256, 256..512, 512..768, 768..1000]);
    for (fragment, range) in fragments.iter().zip(ranges) {
        let range = range.start as usize..range.end as usize;
        let held = range.len();
        assert_eq!(&fragment[..held], &data[range]);
        assert!(fragment[held..].iter().all(|&b| b == 0), "padding is zeros");
    }

    // A short stripe leaves its last data fragments as padding only.
    let fragments = codec.encode(Geometry::RS_8_2, &[0xab; 65]).unwrap();
    assert_eq!(codec.data_range(Geometry::RS_8_2, 65, 1), Ok(64..65));
    assert_eq!(codec.data_range(Geometry::RS_8_2, 65, 7), Ok(65..65));
    assert!(fragments[7].iter().all(|&b| b == 0));
}

#[test]
fn mismatched_fragments_are_errors() {
    let codec = rs();
    let geometry = Geometry::RS_3_2;
    let data = sample(500, 3);
    let len = data.len() as u64;
    let fragments = codec.encode(geometry, &data).unwrap();
    let all = without(&fragments, 0);

    // Too few or too many slots.
    assert_eq!(
        codec.decode(geometry, len, &all[..4]),
        Err(EcError::FragmentCount {
            expected: 5,
            actual: 4
        })
    );
    let mut six = all.clone();
    six.push(None);
    assert_eq!(
        codec.reconstruct(geometry, len, &six),
        Err(EcError::FragmentCount {
            expected: 5,
            actual: 6
        })
    );

    // A truncated fragment.
    let mut short = all.clone();
    short[3] = Some(&fragments[3][..100]);
    assert_eq!(
        codec.decode(geometry, len, &short),
        Err(EcError::FragmentLength {
            index: 3,
            expected: 192,
            actual: 100
        })
    );

    // Fragments of a stripe with a different data length.
    assert_eq!(
        codec.decode(geometry, 2000, &all),
        Err(EcError::FragmentLength {
            index: 0,
            expected: 704,
            actual: 192
        })
    );

    // The data length cannot be zero, or overflow.
    assert_eq!(codec.decode(geometry, 0, &all), Err(EcError::EmptyStripe));
    assert_eq!(
        codec.decode(geometry, u64::MAX, &all),
        Err(EcError::StripeTooLarge { data_len: u64::MAX })
    );
    assert_eq!(codec.encode(geometry, &[]), Err(EcError::EmptyStripe));

    // A parity fragment holds no range of the data.
    assert_eq!(
        codec.data_range(geometry, len, 3),
        Err(EcError::NotADataFragment {
            index: 3,
            data_fragments: 3
        })
    );
    assert_eq!(codec.data_range(geometry, 0, 0), Err(EcError::EmptyStripe));
}

#[test]
fn errors_explain_themselves() {
    let messages = [
        (
            EcError::InvalidGeometry { data: 0, parity: 2 },
            "invalid geometry 0+2",
        ),
        (
            EcError::UnknownCodec(CodecId::new(9)),
            "unknown erasure codec 9",
        ),
        (
            EcError::NotEnoughFragments {
                needed: 4,
                available: 3,
            },
            "3 fragments available, at least 4 needed",
        ),
        (
            EcError::FragmentLength {
                index: 1,
                expected: 64,
                actual: 3,
            },
            "fragment 1 has 3 bytes, expected 64",
        ),
    ];
    for (error, message) in messages {
        assert_eq!(error.to_string(), message);
    }
}

fn design_geometry() -> impl Strategy<Value = Geometry> {
    prop::sample::select(Geometry::DESIGN_TABLE.to_vec())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Any data size encodes to equal-length fragments laid out by
    /// `data_range`, and decodes after any `m` losses.
    #[test]
    fn any_data_size_round_trips(
        geometry in design_geometry(),
        data in prop::collection::vec(any::<u8>(), 1..40_000),
        lost in prop::collection::vec(any::<prop::sample::Index>(), 0..=2),
    ) {
        let codec = current_codec();
        let data_len = data.len() as u64;
        let fragments = codec.encode(geometry, &data).unwrap();
        let fragment_len = codec.fragment_len(geometry, data_len).unwrap();
        prop_assert_eq!(fragment_len % 64, 0);
        prop_assert!(fragment_len * geometry.data_fragments() as u64 >= data_len);
        prop_assert!(fragment_len < data_len.div_ceil(geometry.data_fragments() as u64) + 64);

        let mut laid_out = Vec::with_capacity(data.len());
        for (index, fragment) in fragments.iter().enumerate().take(geometry.data_fragments()) {
            prop_assert_eq!(fragment.len() as u64, fragment_len);
            let range = codec.data_range(geometry, data_len, index).unwrap();
            laid_out.extend_from_slice(&fragment[..(range.end - range.start) as usize]);
        }
        prop_assert_eq!(&laid_out, &data);

        let lost = lost
            .iter()
            .fold(0u32, |mask, i| mask | 1 << i.index(geometry.total_fragments()));
        let slots = without(&fragments, lost);
        prop_assert_eq!(codec.decode(geometry, data_len, &slots).unwrap(), data);
        prop_assert_eq!(codec.reconstruct(geometry, data_len, &slots).unwrap(), fragments);
    }
}
