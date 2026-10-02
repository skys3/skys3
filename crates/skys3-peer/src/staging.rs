//! What a destination holds of the objects sources are sending it: the
//! staging of each write identity (design §7.8).
//!
//! Every frame a source sends is staged as an `EXTENT` record of the
//! object's key, in the key's shard, by the shard's primary, which answers
//! once the record is durable on every member of the shard. [`Staging`]
//! keeps the index of those records on the node that accepted the stream:
//! for each write identity, its destination bucket and key and, for each
//! piece, the ranges that are durable and the extents that hold them. It
//! does no I/O itself; [`StagingService`](crate::StagingService) relays the
//! frames and reports what becomes durable.
//!
//! - **Trimming.** A frame is staged only where the staging neither holds
//!   its bytes nor has them in flight, so a piece's extents never overlap
//!   and a `COMMIT` can list them in offset order ([`StagedObject`]).
//! - **Quota.** What each source cluster stages stays within
//!   `peer_staging_quota_bytes`. Each staging, and each part of a frame in
//!   flight or durable, is charged its bytes but at least
//!   [`MIN_STAGING_CHARGE`], so the index's memory is bounded by the quota
//!   however small the frames. A `BEGIN` or frame that would pass it is
//!   refused, and a frame discards its identity's staging.
//! - **Generations.** Each staging has a generation, which an append
//!   carries from [`Staging::admit`] to [`Staging::settle`], so an append
//!   that outlives its staging never changes one that replaced it.
//! - **Expiry.** Staging that sees no `BEGIN` or `DATA` for
//!   `peer_staging_ttl_seconds` is discarded.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::PeeringConfig;
use skys3_log::record::{ExtentRef, MAX_EXTENTS};
use skys3_types::limits::MIN_STAGING_CHARGE;
use skys3_types::{BucketName, ClusterId, WriteIdentity};
use tokio::time::Instant;

use crate::message::{
    AbortReason, Begin, Data, MAX_REPORTED_PIECES, MAX_REPORTED_RANGES, StagedRanges,
};
use crate::ranges::ByteRanges;

/// The longest time between two sweeps for expired staging. Sweeps run
/// while staging is used, at most this often, or every
/// `peer_staging_ttl_seconds` if that is shorter.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// The bounds on what a destination stages, from `[peering]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingLimits {
    /// `peer_staging_quota_bytes`: the most one source cluster may stage
    /// on this node, each staging and each staged part charged at least
    /// [`MIN_STAGING_CHARGE`].
    pub quota_bytes: u64,
    /// `peer_staging_ttl_seconds`: how long staging may go without a
    /// `BEGIN` or `DATA` before it is discarded.
    pub ttl: Duration,
}

impl StagingLimits {
    /// The limits of the `[peering]` section.
    #[must_use]
    pub fn from_config(config: &PeeringConfig) -> Self {
        Self {
            quota_bytes: config.peer_staging_quota_bytes,
            ttl: config.peer_staging_ttl(),
        }
    }
}

/// The staging of every write identity whose frames this node relays.
/// Streams of one identity share it, so a stream that replaces one lost
/// with its connection finds what the old one staged.
#[derive(Debug)]
pub struct Staging {
    limits: StagingLimits,
    table: Mutex<Table>,
}

#[derive(Debug, Default)]
struct Table {
    stagings: HashMap<WriteIdentity, Entry>,
    /// What each source cluster's staging is charged.
    used: HashMap<ClusterId, u64>,
    /// The generation the next staging gets.
    next_generation: u64,
    /// When expired staging is next looked for.
    next_sweep: Option<Instant>,
}

/// The staging of one write identity.
#[derive(Debug)]
struct Entry {
    bucket: BucketName,
    key: String,
    pieces: BTreeMap<u64, Piece>,
    /// What the staging is charged against the quota.
    bytes: u64,
    /// Tells this staging apart from an earlier one of its identity.
    generation: u64,
    /// The last `BEGIN` or `DATA`.
    touched: Instant,
}

/// The staging of one piece.
#[derive(Debug, Default)]
struct Piece {
    /// The bytes durable on every member of the key's shard.
    durable: ByteRanges,
    /// The extents that hold them, by offset.
    extents: BTreeMap<u64, ExtentRef>,
    /// The ranges being staged, from start to end. They overlap neither
    /// each other nor `durable`.
    pending: BTreeMap<u64, u64>,
}

impl Piece {
    /// The parts of `range` that the piece neither holds nor has in
    /// flight, in order.
    fn uncovered(&self, range: Range<u64>) -> Vec<Range<u64>> {
        let durable = self.durable.as_slice();
        let first = durable.partition_point(|held| held.end <= range.start);
        let held: ByteRanges = durable[first..]
            .iter()
            .take_while(|held| held.start < range.end)
            .cloned()
            .chain(
                // In flight ranges do not overlap, so only the last one to
                // start before `range` can reach into it.
                self.pending
                    .range(..range.start)
                    .next_back()
                    .into_iter()
                    .chain(self.pending.range(range.clone()))
                    .map(|(&start, &end)| start..end),
            )
            .collect();
        held.missing(range.end)
            .into_iter()
            .filter_map(|gap| {
                let start = gap.start.max(range.start);
                (start < gap.end).then_some(start..gap.end)
            })
            .collect()
    }
}

/// The charge for staging `len` bytes in one extent.
fn charge(len: u64) -> u64 {
    len.max(MIN_STAGING_CHARGE)
}

/// Parts of a frame that [`Staging::admit`] put in flight, for the
/// generation of staging they belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    /// The generation of the staging, which [`Staging::settle`] takes.
    pub generation: u64,
    /// The parts, in order: the frame's range less what the staging holds
    /// or has in flight. Empty if it holds every byte already.
    pub ranges: Vec<Range<u64>>,
}

/// What a source staged for one write identity, as a `COMMIT` publishes
/// it: the destination bucket and key, and the extents of each piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedObject {
    /// The destination bucket.
    pub bucket: BucketName,
    /// The key in the destination bucket.
    pub key: String,
    /// The durable extents of each piece, by piece and then offset.
    pub pieces: BTreeMap<u64, BTreeMap<u64, ExtentRef>>,
}

impl StagedObject {
    /// The extents of `piece` in order, if they hold exactly its `size`
    /// bytes from offset 0 with no gap: what a `PUT` that publishes the
    /// piece references. `None` if the piece is incomplete or holds bytes
    /// past `size`.
    #[must_use]
    pub fn extents(&self, piece: u64, size: u64) -> Option<Vec<ExtentRef>> {
        let extents = self.pieces.get(&piece);
        let mut next = 0;
        let mut out = Vec::new();
        for (&offset, extent) in extents.into_iter().flatten() {
            if offset != next {
                return None;
            }
            next += u64::from(extent.len);
            out.push(*extent);
        }
        (next == size).then_some(out)
    }
}

impl Staging {
    /// An empty staging table bounded by `limits`.
    #[must_use]
    pub fn new(limits: StagingLimits) -> Self {
        Self {
            limits,
            table: Mutex::default(),
        }
    }

    /// The limits the table enforces.
    #[must_use]
    pub fn limits(&self) -> StagingLimits {
        self.limits
    }

    fn table(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens or resumes the staging of `begin`'s identity, and returns its
    /// `RESUME`: every durable range of the staging, empty for new
    /// staging.
    ///
    /// New staging is charged [`MIN_STAGING_CHARGE`].
    ///
    /// # Errors
    ///
    /// [`AbortReason::Refused`] if the identity is already staged for
    /// another bucket or key, and that staging stays; and
    /// [`AbortReason::QuotaExceeded`] if new staging would take its source
    /// past `peer_staging_quota_bytes`.
    pub fn begin(&self, begin: &Begin, now: Instant) -> Result<StagedRanges, AbortReason> {
        let mut table = self.table();
        self.sweep(&mut table, now);
        let Table {
            stagings,
            used,
            next_generation,
            ..
        } = &mut *table;
        if !stagings.contains_key(&begin.identity) {
            let source = used.entry(begin.identity.cluster.clone()).or_default();
            if source.saturating_add(MIN_STAGING_CHARGE) > self.limits.quota_bytes {
                return Err(AbortReason::QuotaExceeded);
            }
            *source += MIN_STAGING_CHARGE;
            *next_generation += 1;
            let entry = Entry {
                bucket: begin.bucket.clone(),
                key: begin.key.clone(),
                pieces: BTreeMap::new(),
                bytes: MIN_STAGING_CHARGE,
                generation: *next_generation,
                touched: now,
            };
            stagings.insert(begin.identity.clone(), entry);
        }
        let entry = stagings
            .get_mut(&begin.identity)
            .expect("the staging was just found or added");
        if entry.bucket != begin.bucket || entry.key != begin.key {
            return Err(AbortReason::Refused);
        }
        entry.touched = now;
        Ok(report(&begin.identity, entry, entry.pieces.keys().copied()))
    }

    /// Admits `data` to the staging of `identity`: puts the parts of its
    /// range that the staging neither holds nor has in flight in flight,
    /// and returns them with the staging's generation. The caller stages
    /// each and then reports it with [`Staging::settle`].
    ///
    /// # Errors
    ///
    /// Why the frame cannot be staged. The staging is then gone:
    /// [`AbortReason::Expired`] if there is none, as after it expired or
    /// the source aborted it; [`AbortReason::QuotaExceeded`] if the frame
    /// would take its source past `peer_staging_quota_bytes`, which
    /// discards it; and [`AbortReason::Refused`] if the piece would need
    /// more extents than a `PUT` references, which discards it too.
    pub fn admit(
        &self,
        identity: &WriteIdentity,
        data: &Data,
        now: Instant,
    ) -> Result<Admitted, AbortReason> {
        let mut table = self.table();
        self.sweep(&mut table, now);
        let Table { stagings, used, .. } = &mut *table;
        let entry = stagings.get_mut(identity).ok_or(AbortReason::Expired)?;
        entry.touched = now;
        let end = data.offset + data.bytes.len() as u64;
        let piece = entry.pieces.entry(data.piece).or_default();
        let ranges = piece.uncovered(data.offset..end);
        let extents = piece.extents.len() + piece.pending.len() + ranges.len();
        let bytes: u64 = ranges
            .iter()
            .map(|range| charge(range.end - range.start))
            .sum();
        let source = used.entry(identity.cluster.clone()).or_default();
        let refusal = if extents > MAX_EXTENTS {
            Some(AbortReason::Refused)
        } else if source.saturating_add(bytes) > self.limits.quota_bytes {
            Some(AbortReason::QuotaExceeded)
        } else {
            None
        };
        if let Some(reason) = refusal {
            table.remove(identity);
            return Err(reason);
        }
        *source += bytes;
        entry.bytes += bytes;
        for range in &ranges {
            piece.pending.insert(range.start, range.end);
        }
        Ok(Admitted {
            generation: entry.generation,
            ranges,
        })
    }

    /// Settles `range` of `piece`, which [`Staging::admit`] returned for
    /// staging of `generation`: it is durable in `extent`, or, with `None`,
    /// it failed and is no longer in flight. Returns whether the piece's
    /// durable ranges grew, so that a `DURABLE` should report them.
    /// Staging discarded in the meantime is not revived, and staging that
    /// replaced it, of another generation, is left alone.
    pub fn settle(
        &self,
        identity: &WriteIdentity,
        generation: u64,
        piece: u64,
        range: Range<u64>,
        extent: Option<ExtentRef>,
    ) -> bool {
        let mut table = self.table();
        let Table { stagings, used, .. } = &mut *table;
        let Some(entry) = stagings
            .get_mut(identity)
            .filter(|entry| entry.generation == generation)
        else {
            return false;
        };
        let Some(staged) = entry.pieces.get_mut(&piece) else {
            return false;
        };
        if staged.pending.remove(&range.start) != Some(range.end) {
            return false;
        }
        if let Some(extent) = extent {
            staged.durable.insert(range.clone());
            staged.extents.insert(range.start, extent);
            return true;
        }
        let len = charge(range.end - range.start);
        entry.bytes -= len;
        release(used, &identity.cluster, len);
        false
    }

    /// The `DURABLE` of `pieces` of `identity`'s staging: each piece with
    /// all of its durable ranges, or `None` if the staging is gone.
    pub fn report(
        &self,
        identity: &WriteIdentity,
        pieces: impl IntoIterator<Item = u64>,
    ) -> Option<StagedRanges> {
        let table = self.table();
        let entry = table.stagings.get(identity)?;
        Some(report(identity, entry, pieces))
    }

    /// Discards the staging of `identity`, as an `ABORT` asks, and returns
    /// whether there was any. Frames in flight for it settle into nothing;
    /// the extents they and the staging wrote are reclaimed as
    /// unreferenced (design §10.3).
    pub fn discard(&self, identity: &WriteIdentity) -> bool {
        self.table().remove(identity)
    }

    /// Discards every staging that saw no `BEGIN` or `DATA` within the
    /// TTL before `now`, and returns their identities.
    pub fn expire(&self, now: Instant) -> Vec<WriteIdentity> {
        self.table().expire(now, self.limits.ttl)
    }

    /// What is staged durably for `identity`, as a `COMMIT` publishes it.
    #[must_use]
    pub fn staged(&self, identity: &WriteIdentity) -> Option<StagedObject> {
        let table = self.table();
        let entry = table.stagings.get(identity)?;
        Some(StagedObject {
            bucket: entry.bucket.clone(),
            key: entry.key.clone(),
            pieces: entry
                .pieces
                .iter()
                .map(|(&piece, staged)| (piece, staged.extents.clone()))
                .collect(),
        })
    }

    /// What the staging of `source` on this node is charged.
    #[must_use]
    pub fn used(&self, source: &ClusterId) -> u64 {
        self.table().used.get(source).copied().unwrap_or(0)
    }

    /// The number of write identities staged.
    #[must_use]
    pub fn len(&self) -> usize {
        self.table().stagings.len()
    }

    /// Whether nothing is staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Expires staging if a sweep is due.
    fn sweep(&self, table: &mut Table, now: Instant) {
        if table.next_sweep.is_some_and(|next| now < next) {
            return;
        }
        table.next_sweep = Some(now + self.limits.ttl.min(SWEEP_INTERVAL));
        table.expire(now, self.limits.ttl);
    }
}

impl Table {
    fn remove(&mut self, identity: &WriteIdentity) -> bool {
        let Some(entry) = self.stagings.remove(identity) else {
            return false;
        };
        release(&mut self.used, &identity.cluster, entry.bytes);
        true
    }

    fn expire(&mut self, now: Instant, ttl: Duration) -> Vec<WriteIdentity> {
        let expired: Vec<_> = self
            .stagings
            .iter()
            .filter(|(_, entry)| now.saturating_duration_since(entry.touched) >= ttl)
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in &expired {
            self.remove(identity);
        }
        expired
    }
}

/// Returns `len` bytes of `source`'s quota.
fn release(used: &mut HashMap<ClusterId, u64>, source: &ClusterId, len: u64) {
    if let Some(bytes) = used.get_mut(source) {
        *bytes -= len;
        if *bytes == 0 {
            used.remove(source);
        }
    }
}

/// The durable ranges of `pieces` of `entry`, as many as one message
/// reports. Pieces with nothing durable are left out.
fn report(
    identity: &WriteIdentity,
    entry: &Entry,
    pieces: impl IntoIterator<Item = u64>,
) -> StagedRanges {
    let mut reported = BTreeMap::new();
    let mut ranges = 0;
    for piece in pieces {
        let Some(durable) = entry.pieces.get(&piece).map(|staged| &staged.durable) else {
            continue;
        };
        if durable.is_empty() || reported.contains_key(&piece) {
            continue;
        }
        // A subset is safe: the source resends whatever it is not told is
        // durable.
        if reported.len() == MAX_REPORTED_PIECES || ranges + durable.len() > MAX_REPORTED_RANGES {
            break;
        }
        ranges += durable.len();
        reported.insert(piece, durable.clone());
    }
    StagedRanges {
        identity: identity.clone(),
        pieces: reported,
    }
}

#[cfg(test)]
// The tests spell out sets of one range on purpose.
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use bytes::Bytes;
    use skys3_types::{Epoch, EpochSeq, Seq};

    use super::*;

    /// The least charge, which the tests count in.
    const C: u64 = MIN_STAGING_CHARGE;

    const LIMITS: StagingLimits = StagingLimits {
        quota_bytes: 16 * C,
        ttl: Duration::from_secs(600),
    };

    fn identity(cluster: &str, seq: u64) -> WriteIdentity {
        format!("{cluster}/b-src/0/1.{seq}").parse().unwrap()
    }

    fn begin(identity: &WriteIdentity, key: &str) -> Begin {
        Begin {
            identity: identity.clone(),
            bucket: BucketName::new("archive").unwrap(),
            key: key.to_owned(),
        }
    }

    fn data(piece: u64, range: Range<u64>) -> Data {
        Data {
            piece,
            offset: range.start,
            bytes: Bytes::from(vec![7; (range.end - range.start) as usize]),
        }
    }

    fn extent(seq: u64, range: &Range<u64>) -> ExtentRef {
        ExtentRef {
            position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
            len: u32::try_from(range.end - range.start).unwrap(),
        }
    }

    /// The parts of `range` of `piece` that `admit` puts in flight.
    fn admit(
        staging: &Staging,
        id: &WriteIdentity,
        piece: u64,
        range: Range<u64>,
        now: Instant,
    ) -> Result<Vec<Range<u64>>, AbortReason> {
        Ok(staging.admit(id, &data(piece, range), now)?.ranges)
    }

    /// Admits `range` of `piece` and settles every part of it as durable.
    fn stage(staging: &Staging, id: &WriteIdentity, piece: u64, range: Range<u64>, now: Instant) {
        let admitted = staging.admit(id, &data(piece, range), now).unwrap();
        for (seq, part) in (1..).zip(admitted.ranges) {
            let extent = extent(seq, &part);
            assert!(staging.settle(id, admitted.generation, piece, part, Some(extent)));
        }
    }

    fn ranges(report: &StagedRanges, piece: u64) -> Vec<Range<u64>> {
        report.pieces[&piece].as_slice().to_vec()
    }

    #[test]
    fn begin_opens_and_resumes_staging_of_one_bucket_and_key() {
        let staging = Staging::new(LIMITS);
        assert_eq!(staging.limits(), LIMITS);
        let id = identity("us", 1);
        let now = Instant::now();
        let resume = staging.begin(&begin(&id, "k"), now).unwrap();
        assert_eq!(resume.identity, id);
        assert!(resume.pieces.is_empty());
        stage(&staging, &id, 1, 0..100, now);
        // A reconnect's BEGIN finds the staging; another key is refused and
        // leaves it.
        let resume = staging.begin(&begin(&id, "k"), now).unwrap();
        assert_eq!(ranges(&resume, 1), [0..100]);
        assert_eq!(
            staging.begin(&begin(&id, "other"), now),
            Err(AbortReason::Refused)
        );
        let mut elsewhere = begin(&id, "k");
        elsewhere.bucket = BucketName::new("elsewhere").unwrap();
        assert_eq!(staging.begin(&elsewhere, now), Err(AbortReason::Refused));
        assert_eq!(staging.len(), 1);
        // The staging and its one extent, each charged the least charge.
        assert_eq!(staging.used(&id.cluster), 2 * C);
        // DATA without staging is refused as expired.
        let unknown = identity("us", 2);
        assert_eq!(
            admit(&staging, &unknown, 1, 0..10, now),
            Err(AbortReason::Expired)
        );
        assert!(!staging.settle(&unknown, 1, 1, 0..10, None));
        assert_eq!(staging.report(&unknown, [1]), None);
        assert_eq!(staging.staged(&unknown), None);
    }

    #[test]
    fn frames_are_trimmed_to_what_is_neither_durable_nor_in_flight() {
        let staging = Staging::new(LIMITS);
        let id = identity("us", 1);
        let now = Instant::now();
        staging.begin(&begin(&id, "k"), now).unwrap();
        stage(&staging, &id, 1, 100..200, now);
        let in_flight = staging.admit(&id, &data(1, 300..400), now).unwrap();
        assert_eq!(in_flight.ranges, [300..400]);
        let generation = in_flight.generation;
        // A frame across both stages only the gaps around them.
        let parts = admit(&staging, &id, 1, 50..450, now).unwrap();
        assert_eq!(parts, [50..100, 200..300, 400..450]);
        assert_eq!(admit(&staging, &id, 1, 120..180, now).unwrap(), []);
        assert_eq!(admit(&staging, &id, 1, 310..320, now).unwrap(), []);
        // Another piece is staged on its own.
        assert_eq!(admit(&staging, &id, 2, 0..10, now).unwrap(), [0..10]);
        // The staging, and six parts in flight or durable.
        assert_eq!(staging.used(&id.cluster), 7 * C);

        // A failed part is in flight no more, and its charge is returned.
        assert!(!staging.settle(&id, generation, 1, 300..400, None));
        assert_eq!(staging.used(&id.cluster), 6 * C);
        assert_eq!(admit(&staging, &id, 1, 300..350, now).unwrap(), [300..350]);
        // Settling what is not in flight changes nothing.
        let stray = Some(extent(9, &(300..400)));
        assert!(!staging.settle(&id, generation, 1, 300..400, stray));
        assert!(!staging.settle(&id, generation, 9, 0..1, None));
        let first = Some(extent(2, &(50..100)));
        assert!(staging.settle(&id, generation, 1, 50..100, first));
        let second = Some(extent(3, &(200..300)));
        assert!(staging.settle(&id, generation, 1, 200..300, second));
        let report = staging.report(&id, [1, 2, 3]).unwrap();
        // Piece 2 has nothing durable, and piece 3 does not exist.
        assert_eq!(report.pieces.len(), 1);
        assert_eq!(ranges(&report, 1), [50..300]);

        let staged = staging.staged(&id).unwrap();
        assert_eq!(
            (staged.bucket.as_str(), staged.key.as_str()),
            ("archive", "k")
        );
        assert_eq!(
            staged.pieces[&1].keys().copied().collect::<Vec<_>>(),
            [50, 100, 200]
        );
        // Not from offset 0, so incomplete.
        assert_eq!(staged.extents(1, 300), None);
        assert_eq!(staged.extents(7, 0), Some(vec![]));
    }

    #[test]
    fn a_complete_piece_lists_its_extents_in_order() {
        let staging = Staging::new(LIMITS);
        let id = identity("us", 1);
        let now = Instant::now();
        staging.begin(&begin(&id, "k"), now).unwrap();
        for range in [200..300, 0..100, 100..200] {
            stage(&staging, &id, 4, range, now);
        }
        let staged = staging.staged(&id).unwrap();
        let extents = staged.extents(4, 300).unwrap();
        assert_eq!(extents.iter().map(|e| e.len).sum::<u32>(), 300);
        // The size must match exactly.
        assert_eq!(staged.extents(4, 250), None);
        assert_eq!(staged.extents(4, 400), None);
        assert!(staging.discard(&id));
        assert!(!staging.discard(&id));
        assert!(staging.is_empty());
        assert_eq!(staging.used(&id.cluster), 0);
    }

    #[test]
    fn the_quota_bounds_each_source_and_discards_the_staging_that_passes_it() {
        let staging = Staging::new(StagingLimits {
            quota_bytes: 8 * C,
            ..LIMITS
        });
        let (first, second) = (identity("us", 1), identity("us", 2));
        let other = identity("ap", 1);
        let now = Instant::now();
        for id in [&first, &second, &other] {
            staging.begin(&begin(id, "k"), now).unwrap();
        }
        stage(&staging, &first, 1, 0..3 * C, now);
        stage(&staging, &other, 1, 0..7 * C, now);
        // In flight counts too.
        assert_eq!(
            admit(&staging, &second, 1, 0..2 * C, now).unwrap(),
            [0..2 * C]
        );
        let generation = staging
            .admit(&second, &data(1, 0..1), now)
            .unwrap()
            .generation;
        assert_eq!(
            admit(&staging, &second, 1, 2 * C..4 * C, now),
            Err(AbortReason::QuotaExceeded)
        );
        // Only the staging that passed it is discarded.
        assert_eq!(staging.staged(&second), None);
        assert_eq!(staging.used(&first.cluster), 4 * C);
        assert_eq!(staging.used(&other.cluster), 8 * C);
        // Its frame in flight settles into nothing.
        let late = Some(extent(1, &(0..2 * C)));
        assert!(!staging.settle(&second, generation, 1, 0..2 * C, late));
        assert_eq!(staging.used(&first.cluster), 4 * C);
        // Bytes the staging already holds cost nothing again.
        assert_eq!(admit(&staging, &first, 1, 0..3 * C, now).unwrap(), []);
        assert_eq!(
            admit(&staging, &first, 1, 3 * C..7 * C, now).unwrap(),
            [3 * C..7 * C]
        );
        // A source at its quota opens no new staging.
        assert_eq!(
            staging.begin(&begin(&identity("ap", 2), "k"), now),
            Err(AbortReason::QuotaExceeded)
        );
        assert_eq!(staging.len(), 2);
    }

    #[test]
    fn staging_without_data_is_charged_against_the_quota() {
        let staging = Staging::new(StagingLimits {
            quota_bytes: 4 * C,
            ..LIMITS
        });
        let now = Instant::now();
        // A source that only sends BEGINs runs out of quota, not memory.
        for seq in 1..=4 {
            staging
                .begin(&begin(&identity("us", seq), "k"), now)
                .unwrap();
        }
        assert_eq!(
            staging.begin(&begin(&identity("us", 5), "k"), now),
            Err(AbortReason::QuotaExceeded)
        );
        assert_eq!(staging.len(), 4);
        // Tiny frames are charged the least charge each.
        assert_eq!(
            admit(&staging, &identity("us", 1), 1, 0..1, now),
            Err(AbortReason::QuotaExceeded)
        );
        // Another source has its own quota, and resuming costs nothing.
        staging.begin(&begin(&identity("ap", 1), "k"), now).unwrap();
        staging.begin(&begin(&identity("us", 2), "k"), now).unwrap();
        // Discarding staging returns its charge.
        assert!(staging.discard(&identity("us", 2)));
        staging.begin(&begin(&identity("us", 5), "k"), now).unwrap();
    }

    #[test]
    fn an_append_that_outlives_its_staging_leaves_the_replacement_alone() {
        let staging = Staging::new(LIMITS);
        let id = identity("us", 1);
        let now = Instant::now();
        staging.begin(&begin(&id, "k"), now).unwrap();
        let old = staging.admit(&id, &data(1, 0..10), now).unwrap();
        // The source aborts and starts over while the append runs.
        assert!(staging.discard(&id));
        staging.begin(&begin(&id, "k"), now).unwrap();
        let new = staging.admit(&id, &data(1, 0..10), now).unwrap();
        assert_eq!(new.ranges, [0..10]);
        assert_ne!(new.generation, old.generation);
        let used = staging.used(&id.cluster);
        // The old append fails: the new one stays in flight.
        assert!(!staging.settle(&id, old.generation, 1, 0..10, None));
        assert_eq!(admit(&staging, &id, 1, 0..10, now).unwrap(), []);
        assert_eq!(staging.used(&id.cluster), used);
        // The old append succeeds: its extent is not the new staging's.
        let stale = Some(extent(1, &(0..10)));
        assert!(!staging.settle(&id, old.generation, 1, 0..10, stale));
        assert_eq!(staging.report(&id, [1]).unwrap().pieces.len(), 0);
        // The new append lands.
        let fresh = Some(extent(2, &(0..10)));
        assert!(staging.settle(&id, new.generation, 1, 0..10, fresh));
        let extents = staging.staged(&id).unwrap().extents(1, 10).unwrap();
        assert_eq!(extents, [extent(2, &(0..10))]);
    }

    #[test]
    fn staging_expires_once_idle_for_the_ttl() {
        let staging = Staging::new(LIMITS);
        let (idle, busy) = (identity("us", 1), identity("us", 2));
        let start = Instant::now();
        staging.begin(&begin(&idle, "k"), start).unwrap();
        staging.begin(&begin(&busy, "k"), start).unwrap();
        stage(&staging, &idle, 1, 0..10, start);
        // DATA keeps staging alive.
        let later = start + LIMITS.ttl / 2;
        stage(&staging, &busy, 1, 0..10, later);
        assert_eq!(
            staging.expire(start + LIMITS.ttl - Duration::from_secs(1)),
            []
        );
        assert_eq!(
            staging.expire(start + LIMITS.ttl),
            std::slice::from_ref(&idle)
        );
        assert_eq!(staging.used(&idle.cluster), 2 * C);
        // Sweeps run as staging is used, and DATA that finds its staging
        // gone is told it expired.
        let swept = later + LIMITS.ttl + SWEEP_INTERVAL;
        assert_eq!(
            admit(&staging, &busy, 1, 10..20, swept),
            Err(AbortReason::Expired)
        );
        assert!(staging.is_empty());
        assert_eq!(staging.used(&busy.cluster), 0);
        // A BEGIN after expiry starts over.
        let resume = staging.begin(&begin(&busy, "k"), swept).unwrap();
        assert!(resume.pieces.is_empty());
        assert_eq!(
            StagingLimits::from_config(&PeeringConfig::default()),
            StagingLimits {
                quota_bytes: 1 << 40,
                ttl: Duration::from_secs(86_400),
            }
        );
    }

    #[test]
    fn reports_and_extent_counts_stay_within_their_limits() {
        let staging = Staging::new(StagingLimits {
            quota_bytes: u64::MAX,
            ttl: LIMITS.ttl,
        });
        let id = identity("us", 1);
        let now = Instant::now();
        staging.begin(&begin(&id, "k"), now).unwrap();
        // More pieces than one message reports: a RESUME reports a subset.
        let pieces = MAX_REPORTED_PIECES as u64 + 2;
        for piece in 0..pieces {
            stage(&staging, &id, piece, 0..1, now);
        }
        let resume = staging.begin(&begin(&id, "k"), now).unwrap();
        assert_eq!(resume.pieces.len(), MAX_REPORTED_PIECES);
        // More ranges than one message reports: whole pieces only.
        let fragmented = Staging::new(staging.limits());
        fragmented.begin(&begin(&id, "k"), now).unwrap();
        let per_piece = MAX_REPORTED_RANGES as u64 / 2;
        for piece in 0..3 {
            for at in 0..per_piece {
                stage(&fragmented, &id, piece, 2 * at..2 * at + 1, now);
            }
        }
        let report = fragmented.report(&id, [0, 1, 2]).unwrap();
        assert_eq!(report.pieces.keys().copied().collect::<Vec<_>>(), [0, 1]);
        // A piece never needs more extents than a PUT references.
        let piece = u64::MAX;
        for at in 0..MAX_EXTENTS as u64 {
            let parts = admit(&staging, &id, piece, 2 * at..2 * at + 1, now);
            assert_eq!(parts.unwrap().len(), 1);
        }
        let past = 2 * MAX_EXTENTS as u64;
        assert_eq!(
            admit(&staging, &id, piece, past..past + 1, now),
            Err(AbortReason::Refused)
        );
        assert!(staging.is_empty());
    }

    /// One connection of a source that streams one piece: the frame size
    /// it uses, and for each part the destination stages, whether its
    /// append fails, and whether it is still in flight when the connection
    /// drops.
    #[derive(Debug, Clone)]
    struct Round {
        frame: u64,
        outcomes: Vec<(bool, bool)>,
    }

    fn rounds() -> impl proptest::strategy::Strategy<Value = Vec<Round>> {
        use proptest::prelude::*;
        let round = (1u64..40, prop::collection::vec(any::<(bool, bool)>(), 64))
            .prop_map(|(frame, outcomes)| Round { frame, outcomes });
        prop::collection::vec(round, 0..6)
    }

    /// The sink of the simulated source: what it stored, by the position it
    /// gave each extent, how often it stored each byte, and what the
    /// stored parts are charged.
    struct Sink {
        body: Vec<u8>,
        stored: BTreeMap<EpochSeq, Vec<u8>>,
        copies: Vec<u8>,
        durable: ByteRanges,
        charged: u64,
    }

    impl Sink {
        /// Stores `range` and settles it as durable.
        fn land(
            &mut self,
            staging: &Staging,
            id: &WriteIdentity,
            generation: u64,
            range: Range<u64>,
        ) -> bool {
            let extent = extent(self.stored.len() as u64 + 1, &range);
            let (start, end) = (range.start as usize, range.end as usize);
            self.stored
                .insert(extent.position, self.body[start..end].to_vec());
            for copies in &mut self.copies[start..end] {
                *copies += 1;
            }
            self.durable.insert(range.clone());
            self.charged += charge(range.end - range.start);
            staging.settle(id, generation, 0, range, Some(extent))
        }
    }

    proptest::proptest! {
        /// A source that loses its connection, with frames failing or
        /// still in flight, and reconnects with other frame sizes: each
        /// reconnect's `RESUME` reports exactly what is durable, the source
        /// resends only the rest, no byte is staged twice, and once a
        /// connection finishes cleanly the piece is complete.
        #[test]
        fn a_reconnect_resends_only_what_is_not_durable(rounds in rounds(), len in 1u64..400) {
            let staging = Staging::new(StagingLimits { quota_bytes: u64::MAX, ttl: LIMITS.ttl });
            let id = identity("us", 1);
            let now = Instant::now();
            let body: Vec<u8> = (0..len).map(|at| (at % 251) as u8).collect();
            let mut sink = Sink {
                body: body.clone(),
                stored: BTreeMap::new(),
                copies: vec![0; body.len()],
                durable: ByteRanges::new(),
                charged: MIN_STAGING_CHARGE,
            };
            let mut in_flight: Vec<(u64, Range<u64>)> = Vec::new();
            let clean = Round { frame: 64, outcomes: vec![(false, false)] };
            for round in rounds.iter().chain([&clean]) {
                // Frames in flight when the connection dropped land only
                // after the source has resent what the RESUME lacks.
                let landing = std::mem::take(&mut in_flight);
                let resume = staging.begin(&begin(&id, "k"), now).unwrap();
                let told = resume.pieces.get(&0).cloned().unwrap_or_default();
                proptest::prop_assert_eq!(&told, &sink.durable);
                let missing = told.missing(len);
                let mut outcomes = round.outcomes.iter().cycle();
                for gap in &missing {
                    for at in (gap.start..gap.end).step_by(round.frame as usize) {
                        let end = (at + round.frame).min(gap.end);
                        let frame = Data {
                            piece: 0,
                            offset: at,
                            bytes: Bytes::copy_from_slice(&body[at as usize..end as usize]),
                        };
                        let admitted = staging.admit(&id, &frame, now).unwrap();
                        let generation = admitted.generation;
                        for part in admitted.ranges {
                            let &(fails, stays) = outcomes.next().unwrap();
                            if stays {
                                in_flight.push((generation, part));
                            } else if fails {
                                staging.settle(&id, generation, 0, part, None);
                            } else {
                                proptest::prop_assert!(sink.land(&staging, &id, generation, part));
                                let report = staging.report(&id, [0]).unwrap();
                                proptest::prop_assert_eq!(&report.pieces[&0], &sink.durable);
                            }
                        }
                    }
                }
                for (generation, range) in landing {
                    sink.land(&staging, &id, generation, range);
                }
            }
            proptest::prop_assert!(sink.copies.iter().all(|&copies| copies == 1));
            proptest::prop_assert_eq!(staging.used(&id.cluster), sink.charged);
            let extents = staging.staged(&id).unwrap().extents(0, len).unwrap();
            let assembled: Vec<u8> = extents
                .iter()
                .flat_map(|extent| sink.stored[&extent.position].clone())
                .collect();
            proptest::prop_assert_eq!(assembled, body);
        }
    }
}
