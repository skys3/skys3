//! Segment classes, identifiers, file names, and record locations.
//!
//! A disk's segment directory holds files named `<class>-<id>.seg`, where
//! `<class>` is `hot` or `bulk` and `<id>` is the segment's [`SegmentId`] as
//! 16 lowercase hexadecimal digits, for example `hot-000000000000002a.seg`.
//! Ids come from one counter per disk, shared by both classes, so the ids
//! order segments by creation and an id names one segment even without its
//! class. Fixed-width ids also sort file names in id order.

use std::fmt;

use crate::record::RecordKind;

/// The class of a segment, which decides the records it holds (§10.1).
///
/// Small records and bulk payload age differently, so they live in
/// separate segments and compaction (M1-22) can reclaim each on its own
/// schedule. Fragment segments for erasure coding are a third class, added
/// with the fragment store (M5-02).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SegmentClass {
    /// Metadata records and `PUT` records with inline payload of up to
    /// `inline_max_bytes`.
    Hot,
    /// `EXTENT` records: the extents of large object bodies.
    Bulk,
}

impl SegmentClass {
    /// Every class, in the order a group commit writes them.
    pub const ALL: [Self; 2] = [Self::Hot, Self::Bulk];

    /// The class of segment that records of `kind` are appended to:
    /// `EXTENT` records go to bulk segments, everything else to hot ones.
    #[must_use]
    pub const fn of(kind: RecordKind) -> Self {
        match kind {
            RecordKind::Extent => Self::Bulk,
            _ => Self::Hot,
        }
    }

    /// The class's file name prefix: `"hot"` or `"bulk"`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Bulk => "bulk",
        }
    }

    /// The class's index in [`SegmentClass::ALL`].
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Hot => 0,
            Self::Bulk => 1,
        }
    }
}

impl fmt::Display for SegmentClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A segment's identifier, unique on its disk.
///
/// Ids increase with each segment a disk creates, across both classes. An
/// id is reused only if a crash loses the segment that had it before any
/// record in it was acknowledged, so a [`RecordLocation`] handed out by the
/// log never names a different segment later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u64);

impl SegmentId {
    /// Returns the id with the value `id`.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the id's value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next id, or `None` once the counter is exhausted.
    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// The file name suffix of every segment.
const SUFFIX: &str = ".seg";

/// The number of hexadecimal digits of the id in a file name.
const ID_DIGITS: usize = 16;

/// Returns the file name of segment `id` of `class`.
///
/// ```
/// use skys3_log::segment::{SegmentClass, SegmentId, file_name, parse_file_name};
///
/// let name = file_name(SegmentClass::Hot, SegmentId::new(42));
/// assert_eq!(name, "hot-000000000000002a.seg");
/// assert_eq!(parse_file_name(&name), Some((SegmentClass::Hot, SegmentId::new(42))));
/// ```
#[must_use]
pub fn file_name(class: SegmentClass, id: SegmentId) -> String {
    format!("{}-{id}{SUFFIX}", class.name())
}

/// Parses a segment file name, as [`file_name`] writes it.
///
/// Returns `None` for any other name, including ids with uppercase digits,
/// so every segment has exactly one name.
#[must_use]
pub fn parse_file_name(name: &str) -> Option<(SegmentClass, SegmentId)> {
    let stem = name.strip_suffix(SUFFIX)?;
    let (prefix, digits) = stem.split_once('-')?;
    let class = SegmentClass::ALL
        .into_iter()
        .find(|class| class.name() == prefix)?;
    let canonical = digits.len() == ID_DIGITS
        && digits
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !canonical {
        return None;
    }
    let id = u64::from_str_radix(digits, 16).ok()?;
    Some((class, SegmentId(id)))
}

/// Where a record is stored: its segment, its offset in that segment, and
/// its length.
///
/// The log returns a location when it acknowledges a record, and reads a
/// record back by its location. The node-local location map (§10.2, M1-03)
/// stores the locations of extents. Locations are node-local: they never
/// appear in shard metadata or on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordLocation {
    /// The segment that holds the record.
    pub segment: SegmentId,
    /// The offset of the record's first byte in the segment.
    pub offset: u64,
    /// The record's length in bytes, header and payload. A record is at most
    /// [`MAX_RECORD_LEN`](crate::MAX_RECORD_LEN) bytes, so the length
    /// fits a `u32`.
    pub len: u32,
}

impl RecordLocation {
    /// Returns the offset just past the record.
    #[must_use]
    pub const fn end(&self) -> u64 {
        // A segment is at most `u64::MAX` bytes, so a stored record ends
        // within that range; saturate rather than wrap on a forged value.
        self.offset.saturating_add(self.len as u64)
    }
}

impl fmt::Display for RecordLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "segment {} at {}+{}",
            self.segment, self.offset, self.len
        )
    }
}

/// A segment the log holds: its id, its class, and its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    /// The segment's id.
    pub id: SegmentId,
    /// The segment's class.
    pub class: SegmentClass,
    /// The segment's length in bytes: every record in it that the log has
    /// written or recovered. Records in the last bytes may still await a
    /// group commit.
    pub len: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_by_kind() {
        assert_eq!(SegmentClass::of(RecordKind::Extent), SegmentClass::Bulk);
        for kind in RecordKind::ALL {
            if kind != RecordKind::Extent {
                assert_eq!(SegmentClass::of(kind), SegmentClass::Hot, "{kind}");
            }
        }
        for (index, class) in SegmentClass::ALL.into_iter().enumerate() {
            assert_eq!(class.index(), index);
            assert_eq!(class.to_string(), class.name());
        }
    }

    #[test]
    fn file_names_round_trip_and_sort_by_id() {
        for class in SegmentClass::ALL {
            for id in [0, 1, 0xff, 0x1234_5678_9abc_def0, u64::MAX] {
                let name = file_name(class, SegmentId::new(id));
                assert_eq!(parse_file_name(&name), Some((class, SegmentId::new(id))));
            }
        }
        let mut names: Vec<_> = [300, 2, 17]
            .map(|id| file_name(SegmentClass::Bulk, SegmentId::new(id)))
            .into();
        names.sort();
        assert_eq!(
            names,
            [
                "bulk-0000000000000002.seg",
                "bulk-0000000000000011.seg",
                "bulk-000000000000012c.seg"
            ]
        );
    }

    #[test]
    fn other_names_are_not_segments() {
        for name in [
            "",
            "hot",
            "hot-.seg",
            "hot-0000000000000001",
            "hot-0000000000000001.log",
            "hot-000000000000001.seg",
            "hot-00000000000000001.seg",
            "hot-000000000000000A.seg",
            "hot-+000000000000001.seg",
            "warm-0000000000000001.seg",
            "hot_0000000000000001.seg",
            "index.redb",
        ] {
            assert_eq!(parse_file_name(name), None, "{name:?}");
        }
    }

    #[test]
    fn ids_and_locations() {
        assert_eq!(SegmentId::new(7).next(), Some(SegmentId::new(8)));
        assert_eq!(SegmentId::new(u64::MAX).next(), None);
        assert_eq!(SegmentId::new(7).get(), 7);
        let location = RecordLocation {
            segment: SegmentId::new(10),
            offset: 100,
            len: 80,
        };
        assert_eq!(location.end(), 180);
        assert_eq!(location.to_string(), "segment 000000000000000a at 100+80");
        let forged = RecordLocation {
            offset: u64::MAX,
            ..location
        };
        assert_eq!(forged.end(), u64::MAX);
    }
}
