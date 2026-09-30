//! Property tests for byte ranges: [`ByteRange::resolve`] agrees with a
//! byte-by-byte reading of RFC 9110, section 14.1.2.

use proptest::prelude::*;
use skys3_remote::ByteRange;

/// The positions a range selects from an object of `size` bytes, one by one.
fn selected(size: u64, selects: impl Fn(u64) -> bool) -> Option<std::ops::Range<u64>> {
    let positions: Vec<u64> = (0..size).filter(|&at| selects(at)).collect();
    let (&first, &last) = (positions.first()?, positions.last()?);
    assert_eq!(positions.len() as u64, last - first + 1, "contiguous");
    Some(first..last + 1)
}

proptest! {
    #[test]
    fn inclusive_ranges(size in 0_u64..64, first in 0_u64..80, len in 0_u64..80) {
        let last = first + len;
        let range = ByteRange::inclusive(first, last).unwrap();
        prop_assert_eq!(range.resolve(size), selected(size, |at| (first..=last).contains(&at)));
        prop_assert_eq!(range.to_string(), format!("bytes={first}-{last}"));
    }

    #[test]
    fn open_ranges(size in 0_u64..64, first in 0_u64..80) {
        let range = ByteRange::from_offset(first);
        prop_assert_eq!(range.resolve(size), selected(size, |at| at >= first));
    }

    #[test]
    fn suffix_ranges(size in 0_u64..64, len in 0_u64..80) {
        let range = ByteRange::suffix(len);
        prop_assert_eq!(range.resolve(size), selected(size, |at| at + len >= size));
    }

    #[test]
    fn ranges_near_the_limits(size in any::<u64>(), first in any::<u64>(), len in any::<u64>()) {
        for range in [
            ByteRange::inclusive(first, first.saturating_add(len)).unwrap(),
            ByteRange::from_offset(first),
            ByteRange::suffix(len),
        ] {
            if let Some(selected) = range.resolve(size) {
                prop_assert!(selected.start < selected.end && selected.end <= size);
            }
        }
    }
}
