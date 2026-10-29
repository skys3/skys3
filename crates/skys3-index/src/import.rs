//! A bucket's namespace import as the index stores it (§9.1): the key
//! ranges its listing streams cover, each with its own checkpoint.

use crate::entry::ImportCheckpoint;

/// The most key ranges an import is split into, one listing stream each.
pub const MAX_IMPORT_RANGES: usize = 256;

/// One key range of a namespace import and how far its listing has got.
///
/// A range holds the keys after the previous range's end, or every key
/// from the start for the first range, up to and including its own `end`.
/// The last range has no end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRange {
    /// The range's last key, without the target's prefix; `None` for the
    /// last range.
    pub end: Option<String>,
    /// How far the range's listing has got. A running range's `after` is a
    /// key of the range, or `None` before its first page.
    pub checkpoint: ImportCheckpoint,
}

/// The key ranges of a bucket's namespace import (§9.1), in key order, each
/// listed by its own stream and checkpointed apart.
///
/// The import has **passed** a key once the range that holds it has: the
/// key is at most the range's checkpoint, or the range is done. Every
/// remote object at a passed key has its `IMPORT` record applied.
///
/// An import of one range is the single-stream import, and stores exactly
/// as one [`ImportCheckpoint`] did. Once every range is done, the import is
/// one done range again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRanges {
    /// At least one range; ends strictly increase, and only the last has
    /// none.
    ranges: Vec<ImportRange>,
}

impl From<ImportCheckpoint> for ImportRanges {
    fn from(checkpoint: ImportCheckpoint) -> Self {
        Self {
            ranges: vec![ImportRange {
                end: None,
                checkpoint,
            }],
        }
    }
}

impl Default for ImportRanges {
    /// One range that has passed nothing.
    fn default() -> Self {
        ImportCheckpoint::Running { after: None }.into()
    }
}

impl ImportRanges {
    /// An import that has passed every key up to `after` and splits the
    /// rest at `splits`: the first range runs from `after` to the first
    /// split point, and each later one from a split point to the next.
    /// Split points at or before `after`, repeated, or beyond
    /// [`MAX_IMPORT_RANGES`] ranges are left out.
    #[must_use]
    pub fn split(after: Option<String>, splits: impl IntoIterator<Item = String>) -> Self {
        let mut ends: Vec<String> = Vec::new();
        for split in splits {
            let later = ends
                .last()
                .or(after.as_ref())
                .is_none_or(|last| split > *last);
            if later && ends.len() + 1 < MAX_IMPORT_RANGES {
                ends.push(split);
            }
        }
        let mut ranges = Vec::with_capacity(ends.len() + 1);
        let mut first = Some(after);
        for end in ends.into_iter().map(Some).chain([None]) {
            ranges.push(ImportRange {
                end,
                checkpoint: ImportCheckpoint::Running {
                    after: first.take().flatten(),
                },
            });
        }
        Self { ranges }
    }

    /// Ranges as decoded or built elsewhere, if they hold the invariants:
    /// between one and [`MAX_IMPORT_RANGES`] ranges, ends strictly
    /// increasing, only the last without one, and each running range's
    /// checkpoint inside the range.
    #[must_use]
    pub fn from_ranges(ranges: Vec<ImportRange>) -> Option<Self> {
        if ranges.is_empty() || ranges.len() > MAX_IMPORT_RANGES {
            return None;
        }
        let last = ranges.len() - 1;
        let mut start: Option<&str> = None;
        for (i, range) in ranges.iter().enumerate() {
            match (&range.end, i == last) {
                (None, true) => {}
                (Some(end), false) if start.is_none_or(|start| end.as_str() > start) => {}
                _ => return None,
            }
            if let ImportCheckpoint::Running { after: Some(after) } = &range.checkpoint {
                let inside = start.is_none_or(|start| after.as_str() > start)
                    && range.end.as_deref().is_none_or(|end| after.as_str() <= end);
                if !inside {
                    return None;
                }
            }
            start = range.end.as_deref();
        }
        Some(Self { ranges })
    }

    /// The ranges, in key order.
    #[must_use]
    pub fn ranges(&self) -> &[ImportRange] {
        &self.ranges
    }

    /// The key range number `index` lists after when it starts: the end
    /// of the range before it, or `None` for the first.
    #[must_use]
    pub fn start(&self, index: usize) -> Option<&str> {
        index
            .checked_sub(1)
            .and_then(|before| self.ranges.get(before))
            .and_then(|range| range.end.as_deref())
    }

    /// Records how far range number `index` has got. Once every range is
    /// done, the import is one done range, and stays done: a done import,
    /// or a range number it does not have, changes nothing.
    pub fn set(&mut self, index: usize, checkpoint: ImportCheckpoint) {
        if self.is_done() {
            return;
        }
        let Some(range) = self.ranges.get_mut(index) else {
            return;
        };
        range.checkpoint = checkpoint;
        if self.is_done() {
            *self = ImportCheckpoint::Done.into();
        }
    }

    /// Whether every range is done.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.ranges
            .iter()
            .all(|range| range.checkpoint == ImportCheckpoint::Done)
    }

    /// Whether the import has passed `key`: the range that holds it has.
    #[must_use]
    pub fn passed(&self, key: &str) -> bool {
        self.ranges
            .iter()
            .find(|range| range.end.as_deref().is_none_or(|end| key <= end))
            .is_some_and(|range| range.checkpoint.passed(key))
    }

    /// How far the import has got without a gap: it has passed every key
    /// up to the returned checkpoint's `after`, and every key if it is
    /// [`ImportCheckpoint::Done`]. Ranges after the first that is still
    /// running may have passed more.
    #[must_use]
    pub fn position(&self) -> ImportCheckpoint {
        let mut start: Option<&str> = None;
        for range in &self.ranges {
            match &range.checkpoint {
                ImportCheckpoint::Done => start = range.end.as_deref(),
                ImportCheckpoint::Running { after } => {
                    return ImportCheckpoint::Running {
                        after: after.as_deref().or(start).map(str::to_owned),
                    };
                }
            }
        }
        ImportCheckpoint::Done
    }

    /// How many ranges are done.
    #[must_use]
    pub fn done(&self) -> usize {
        self.ranges
            .iter()
            .filter(|range| range.checkpoint == ImportCheckpoint::Done)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(after: Option<&str>) -> ImportCheckpoint {
        ImportCheckpoint::Running {
            after: after.map(str::to_owned),
        }
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn split_points_make_ordered_ranges_after_the_checkpoint() {
        let ranges = ImportRanges::split(Some("c".to_owned()), names(&["b", "f", "f", "e", "m"]));
        let ends: Vec<_> = ranges.ranges().iter().map(|r| r.end.as_deref()).collect();
        assert_eq!(ends, [Some("f"), Some("m"), None]);
        assert_eq!(ranges.ranges()[0].checkpoint, running(Some("c")));
        assert_eq!(ranges.ranges()[1].checkpoint, running(None));
        let starts: Vec<_> = (0..4).map(|i| ranges.start(i)).collect();
        assert_eq!(starts, [None, Some("f"), Some("m"), None]);
        assert!(ImportRanges::from_ranges(ranges.ranges().to_vec()).is_some());

        let many = ImportRanges::split(None, (0..1000).map(|n| format!("k{n:04}")));
        assert_eq!(many.ranges().len(), MAX_IMPORT_RANGES);
        assert_eq!(ImportRanges::split(None, []), ImportRanges::default());
    }

    #[test]
    fn a_key_is_passed_once_its_range_is() {
        let mut ranges = ImportRanges::split(None, names(&["f", "m"]));
        assert!(!ranges.passed("a") && !ranges.passed("g") && !ranges.passed("z"));
        assert_eq!(ranges.position(), running(None));
        ranges.set(1, running(Some("h")));
        assert!(ranges.passed("g") && ranges.passed("h") && !ranges.passed("i"));
        assert!(!ranges.passed("f") && !ranges.passed("a"));
        assert_eq!(ranges.position(), running(None));
        ranges.set(0, ImportCheckpoint::Done);
        assert!(ranges.passed("a") && ranges.passed("f"));
        assert_eq!(ranges.position(), running(Some("h")));
        assert_eq!(ranges.done(), 1);
        ranges.set(1, ImportCheckpoint::Done);
        assert_eq!(ranges.position(), running(Some("m")));
        assert!(ranges.passed("m") && !ranges.passed("n"));
        ranges.set(2, running(Some("p")));
        assert!(ranges.passed("p") && !ranges.passed("q") && !ranges.is_done());
        ranges.set(2, ImportCheckpoint::Done);
        assert_eq!(ranges, ImportCheckpoint::Done.into());
        assert_eq!(ranges.position(), ImportCheckpoint::Done);
        assert!(ranges.is_done() && ranges.passed("zzz"));
        // A done import has nothing left to set.
        ranges.set(2, running(None));
        assert!(ranges.is_done());
    }
}
