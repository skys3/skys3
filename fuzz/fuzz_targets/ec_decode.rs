//! Fuzzes erasure decoding and repair (`skys3-ec`), which read fragments
//! from other nodes' disks: malformed or mismatched fragments must be
//! errors, never panics, and real fragments must decode after any `m`
//! losses.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_ec::{CodecId, EcCodec, EcError, Geometry, codec};

fuzz_target!(|input: &[u8]| {
    let [id, k, m, mode, len_lo, len_hi, lost_lo, lost_hi, rest @ ..] = input else {
        return;
    };
    // Mostly codec 1, sometimes an unknown ID.
    let id = CodecId::new(u16::from(*id % 4));
    let codec = match codec(id) {
        Ok(codec) => codec,
        Err(error) => {
            assert_eq!(error, EcError::UnknownCodec(id));
            return;
        }
    };
    // Up to 12+4 = 16 fragments, so `lost` is a 16-bit mask. Zero counts
    // check that a degenerate geometry is rejected.
    let Ok(geometry) = Geometry::new(usize::from(k % 13), usize::from(m % 5)) else {
        return;
    };
    let lost = u16::from_le_bytes([*lost_lo, *lost_hi]);
    let hint = u64::from(u16::from_le_bytes([*len_lo, *len_hi]));
    match mode % 3 {
        0 => real_fragments(codec, geometry, rest, lost),
        1 => garbage_fragments(codec, geometry, hint + 1, rest, lost),
        _ => malformed_fragments(codec, geometry, hint, *mode, rest),
    }
});

/// Encodes `data`, loses the fragments in `lost`, and decodes.
fn real_fragments(codec: &dyn EcCodec, geometry: Geometry, data: &[u8], lost: u16) {
    if data.is_empty() {
        assert_eq!(codec.encode(geometry, data), Err(EcError::EmptyStripe));
        return;
    }
    let fragments = codec.encode(geometry, data).expect("encode");
    let slots: Vec<Option<&[u8]>> = fragments
        .iter()
        .enumerate()
        .map(|(i, f)| (lost & (1 << i) == 0).then_some(f.as_slice()))
        .collect();
    let available = slots.iter().flatten().count();
    let data_len = data.len() as u64;
    if available >= geometry.data_fragments() {
        assert_eq!(
            codec.decode(geometry, data_len, &slots).expect("decode"),
            data
        );
        let rebuilt = codec
            .reconstruct(geometry, data_len, &slots)
            .expect("reconstruct");
        assert_eq!(rebuilt, fragments);
    } else {
        let expected = EcError::NotEnoughFragments {
            needed: geometry.data_fragments(),
            available,
        };
        assert_eq!(codec.decode(geometry, data_len, &slots), Err(expected));
    }
}

/// Fragments of the right length but arbitrary content: decoding succeeds
/// with arbitrary data, and repair agrees with it.
fn garbage_fragments(
    codec: &dyn EcCodec,
    geometry: Geometry,
    data_len: u64,
    bytes: &[u8],
    lost: u16,
) {
    let len = codec
        .fragment_len(geometry, data_len)
        .expect("fragment length") as usize;
    let mut source = bytes.iter().copied().cycle().chain(std::iter::repeat(0));
    let fragments: Vec<Vec<u8>> = (0..geometry.total_fragments())
        .map(|_| source.by_ref().take(len).collect())
        .collect();
    let slots: Vec<Option<&[u8]>> = fragments
        .iter()
        .enumerate()
        .map(|(i, f)| (lost & (1 << i) == 0).then_some(f.as_slice()))
        .collect();
    let decoded = codec.decode(geometry, data_len, &slots);
    let rebuilt = codec.reconstruct(geometry, data_len, &slots);
    match (decoded, rebuilt) {
        (Ok(data), Ok(rebuilt)) => {
            assert_eq!(data.len() as u64, data_len);
            for (slot, fragment) in slots.iter().zip(&rebuilt) {
                if let Some(given) = slot {
                    assert_eq!(given, fragment, "a present fragment changed");
                }
            }
            let laid_out: Vec<u8> = rebuilt[..geometry.data_fragments()].concat();
            assert_eq!(&laid_out[..data.len()], data.as_slice());
        }
        (Err(a), Err(b)) => {
            assert_eq!(a, b);
            assert!(matches!(a, EcError::NotEnoughFragments { .. }), "{a}");
        }
        (decoded, rebuilt) => panic!("decode {decoded:?} but reconstruct {rebuilt:?}"),
    }
}

/// Fragment lists of arbitrary shape: any slot count, any lengths, any data
/// length, including ones that overflow.
fn malformed_fragments(codec: &dyn EcCodec, geometry: Geometry, hint: u64, mode: u8, bytes: &[u8]) {
    let data_len = match mode >> 4 {
        0 => 0,
        1 => u64::MAX - hint,
        _ => hint,
    };
    // Each fragment takes a length byte, then that many bytes; a zero length
    // byte leaves the slot empty.
    let mut slots: Vec<Option<&[u8]>> = Vec::new();
    let mut rest = bytes;
    while let [len, tail @ ..] = rest {
        let len = usize::from(*len).min(tail.len());
        let (fragment, tail) = tail.split_at(len);
        slots.push((len > 0).then_some(fragment));
        rest = tail;
    }
    for result in [
        codec
            .decode(geometry, data_len, &slots)
            .map(|data| data.len() as u64),
        codec
            .reconstruct(geometry, data_len, &slots)
            .map(|f| f.len() as u64),
    ] {
        if let Ok(len) = result {
            assert!(len == data_len || len == geometry.total_fragments() as u64);
        }
    }
    let _ = codec.data_range(geometry, data_len, usize::from(mode & 0xf));
}
