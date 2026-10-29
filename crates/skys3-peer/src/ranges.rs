//! Sets of byte ranges: what a destination holds durably of a staged piece.

use std::ops::Range;

/// A set of byte ranges in canonical form: sorted, nonempty, and neither
/// overlapping nor adjacent, so every set has exactly one representation
/// and one encoding.
///
/// ```
/// use skys3_peer::ByteRanges;
///
/// let mut durable = ByteRanges::new();
/// durable.insert(0..100);
/// durable.insert(200..300);
/// durable.insert(100..150);
/// assert_eq!(durable.as_slice(), [0..150, 200..300]);
/// assert_eq!(durable.missing(400), [150..200, 300..400]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ByteRanges(Vec<Range<u64>>);

impl ByteRanges {
    /// The empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// The set of `ranges`, which must already be canonical, as a peer
    /// sends them.
    ///
    /// # Errors
    ///
    /// The ranges back, unchanged, if one is empty or if they are not
    /// sorted with a gap between neighbors.
    pub fn from_canonical(ranges: Vec<Range<u64>>) -> Result<Self, Vec<Range<u64>>> {
        let canonical = ranges.iter().all(|r| r.start < r.end)
            && ranges.windows(2).all(|w| w[0].end < w[1].start);
        if canonical {
            Ok(Self(ranges))
        } else {
            Err(ranges)
        }
    }

    /// Adds `range`, merging it with the ranges it overlaps or touches. An
    /// empty range changes nothing.
    pub fn insert(&mut self, range: Range<u64>) {
        if range.start >= range.end {
            return;
        }
        // The ranges that end before `range` starts stay, and so do those
        // that start after it ends; everything between merges with it.
        let first = self.0.partition_point(|r| r.end < range.start);
        let last = self.0.partition_point(|r| r.start <= range.end);
        let mut merged = range;
        if first < last {
            merged.start = merged.start.min(self.0[first].start);
            merged.end = merged.end.max(self.0[last - 1].end);
        }
        self.0.splice(first..last, [merged]);
    }

    /// The ranges, in order.
    #[must_use]
    pub fn as_slice(&self) -> &[Range<u64>] {
        &self.0
    }

    /// The number of ranges.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The end of the last range, 0 for the empty set.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.0.last().map_or(0, |r| r.end)
    }

    /// Whether the set holds every byte of `0..len`.
    #[must_use]
    pub fn covers(&self, len: u64) -> bool {
        len == 0 || self.0.first().is_some_and(|r| r.start == 0 && r.end >= len)
    }

    /// The ranges of `0..len` the set does not hold, in order: what a
    /// source resends after a `RESUME`.
    #[must_use]
    pub fn missing(&self, len: u64) -> Vec<Range<u64>> {
        let mut gaps = Vec::new();
        let mut next = 0;
        for range in &self.0 {
            if range.start >= len {
                break;
            }
            if next < range.start {
                gaps.push(next..range.start);
            }
            next = range.end;
        }
        if next < len {
            gaps.push(next..len);
        }
        gaps
    }
}

impl FromIterator<Range<u64>> for ByteRanges {
    fn from_iter<I: IntoIterator<Item = Range<u64>>>(iter: I) -> Self {
        let mut set = Self::new();
        for range in iter {
            set.insert(range);
        }
        set
    }
}

#[cfg(test)]
// The tests spell out sets of one range, and empty and reversed ranges, on
// purpose.
#[allow(clippy::single_range_in_vec_init, clippy::reversed_empty_ranges)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The set as a bitmap over a small space.
    fn bitmap(ranges: &[Range<u64>], len: u64) -> Vec<bool> {
        (0..len)
            .map(|byte| ranges.iter().any(|r| r.contains(&byte)))
            .collect()
    }

    proptest! {
        #[test]
        fn inserting_keeps_the_set_canonical_and_exact(
            ranges in prop::collection::vec((0u64..200, 0u64..40), 0..20),
        ) {
            let ranges: Vec<_> = ranges.into_iter().map(|(start, len)| start..start + len).collect();
            let set: ByteRanges = ranges.iter().cloned().collect();
            prop_assert_eq!(
                ByteRanges::from_canonical(set.as_slice().to_vec()),
                Ok(set.clone())
            );
            prop_assert_eq!(bitmap(set.as_slice(), 256), bitmap(&ranges, 256));
            let missing = set.missing(256);
            let held: Vec<bool> = bitmap(&missing, 256).into_iter().map(|b| !b).collect();
            prop_assert_eq!(held, bitmap(&ranges, 256));
            prop_assert_eq!(set.covers(256), missing.is_empty());
        }
    }

    #[test]
    fn non_canonical_ranges_are_refused() {
        for ranges in [
            vec![0..0],
            vec![5..3],
            vec![0..10, 10..20],
            vec![0..10, 5..20],
            vec![10..20, 0..5],
        ] {
            assert_eq!(ByteRanges::from_canonical(ranges.clone()), Err(ranges));
        }
        let set = ByteRanges::from_canonical(vec![0..10, 11..20]).unwrap();
        assert_eq!(set.len(), 2);
        assert_eq!(set.end(), 20);
        assert!(!set.covers(20));
        assert!(set.covers(10) && set.covers(0));
        assert!(ByteRanges::new().is_empty());
        assert_eq!(ByteRanges::new().end(), 0);
        assert_eq!(set.missing(5), []);
        assert_eq!(set.missing(15), [10..11]);
    }

    #[test]
    fn insert_merges_neighbors_and_ignores_empty_ranges() {
        let mut set = ByteRanges::new();
        set.insert(10..10);
        assert!(set.is_empty());
        set.insert(10..20);
        set.insert(30..40);
        set.insert(50..60);
        set.insert(20..50);
        assert_eq!(set.as_slice(), [10..60]);
        set.insert(0..5);
        set.insert(70..80);
        assert_eq!(set.as_slice(), [0..5, 10..60, 70..80]);
        set.insert(15..20);
        assert_eq!(set.as_slice(), [0..5, 10..60, 70..80]);
        set.insert(0..100);
        assert_eq!(set.as_slice(), [0..100]);
    }
}
