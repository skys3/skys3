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
//! - **Quota.** The bytes staged or in flight for each source cluster stay
//!   within `peer_staging_quota_bytes`. A frame that would pass it discards
//!   its identity's staging.
//! - **Expiry.** Staging that sees no `BEGIN` or `DATA` for
//!   `peer_staging_ttl_seconds` is discarded.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::PeeringConfig;
use skys3_log::record::{ExtentRef, MAX_EXTENTS};
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
    /// `peer_staging_quota_bytes`: the most bytes staged or in flight for
    /// one source cluster on this node.
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
    /// The bytes staged or in flight per source cluster.
    used: HashMap<ClusterId, u64>,
    /// When expired staging is next looked for.
    next_sweep: Option<Instant>,
}

/// The staging of one write identity.
#[derive(Debug)]
struct Entry {
    bucket: BucketName,
    key: String,
    pieces: BTreeMap<u64, Piece>,
    /// The bytes staged or in flight, counted against the quota.
    bytes: u64,
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
    /// # Errors
    ///
    /// [`AbortReason::Refused`] if the identity is already staged for
    /// another bucket or key. That staging stays.
    pub fn begin(&self, begin: &Begin, now: Instant) -> Result<StagedRanges, AbortReason> {
        let mut table = self.table();
        self.sweep(&mut table, now);
        let entry = table
            .stagings
            .entry(begin.identity.clone())
            .or_insert_with(|| Entry {
                bucket: begin.bucket.clone(),
                key: begin.key.clone(),
                pieces: BTreeMap::new(),
                bytes: 0,
                touched: now,
            });
        if entry.bucket != begin.bucket || entry.key != begin.key {
            return Err(AbortReason::Refused);
        }
        entry.touched = now;
        Ok(report(&begin.identity, entry, entry.pieces.keys().copied()))
    }

    /// Admits `data` to the staging of `identity`: returns the parts of its
    /// range that the staging neither holds nor has in flight, now in
    /// flight, which the caller stages and then reports with
    /// [`Staging::settle`]. They are empty if the staging holds every byte
    /// already.
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
    ) -> Result<Vec<Range<u64>>, AbortReason> {
        let mut table = self.table();
        self.sweep(&mut table, now);
        let Table { stagings, used, .. } = &mut *table;
        let entry = stagings.get_mut(identity).ok_or(AbortReason::Expired)?;
        entry.touched = now;
        let end = data.offset + data.bytes.len() as u64;
        let piece = entry.pieces.entry(data.piece).or_default();
        let ranges = piece.uncovered(data.offset..end);
        let extents = piece.extents.len() + piece.pending.len() + ranges.len();
        let bytes: u64 = ranges.iter().map(|range| range.end - range.start).sum();
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
        Ok(ranges)
    }

    /// Settles `range` of `piece`, which [`Staging::admit`] returned: it is
    /// durable in `extent`, or, with `None`, it failed and is no longer in
    /// flight. Returns whether the piece's durable ranges grew, so that a
    /// `DURABLE` should report them. Staging discarded in the meantime is
    /// not revived.
    pub fn settle(
        &self,
        identity: &WriteIdentity,
        piece: u64,
        range: Range<u64>,
        extent: Option<ExtentRef>,
    ) -> bool {
        let mut table = self.table();
        let Table { stagings, used, .. } = &mut *table;
        let Some(entry) = stagings.get_mut(identity) else {
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
        let len = range.end - range.start;
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

    /// The bytes staged or in flight for `source` on this node.
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

    const LIMITS: StagingLimits = StagingLimits {
        quota_bytes: 1000,
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

    /// Admits `range` of `piece` and settles every part of it as durable.
    fn stage(staging: &Staging, id: &WriteIdentity, piece: u64, range: Range<u64>, now: Instant) {
        for (seq, part) in (1..).zip(staging.admit(id, &data(piece, range), now).unwrap()) {
            let extent = extent(seq, &part);
            assert!(staging.settle(id, piece, part, Some(extent)));
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
        assert_eq!(staging.used(&id.cluster), 100);
        // DATA without staging is refused as expired.
        let unknown = identity("us", 2);
        assert_eq!(
            staging.admit(&unknown, &data(1, 0..10), now),
            Err(AbortReason::Expired)
        );
        assert!(!staging.settle(&unknown, 1, 0..10, None));
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
        assert_eq!(in_flight, [300..400]);
        // A frame across both stages only the gaps around them.
        let parts = staging.admit(&id, &data(1, 50..450), now).unwrap();
        assert_eq!(parts, [50..100, 200..300, 400..450]);
        assert_eq!(staging.admit(&id, &data(1, 120..180), now).unwrap(), []);
        assert_eq!(staging.admit(&id, &data(1, 310..320), now).unwrap(), []);
        // Another piece is staged on its own.
        assert_eq!(staging.admit(&id, &data(2, 0..10), now).unwrap(), [0..10]);
        assert_eq!(staging.used(&id.cluster), 100 + 100 + 200 + 10);

        // A failed part is in flight no more, and its bytes are returned.
        assert!(!staging.settle(&id, 1, 300..400, None));
        assert_eq!(staging.used(&id.cluster), 310);
        assert_eq!(
            staging.admit(&id, &data(1, 300..350), now).unwrap(),
            [300..350]
        );
        // Settling what is not in flight changes nothing.
        assert!(!staging.settle(&id, 1, 300..400, Some(extent(9, &(300..400)))));
        assert!(!staging.settle(&id, 9, 0..1, None));
        assert!(staging.settle(&id, 1, 50..100, Some(extent(2, &(50..100)))));
        assert!(staging.settle(&id, 1, 200..300, Some(extent(3, &(200..300)))));
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
        let staging = Staging::new(LIMITS);
        let (first, second) = (identity("us", 1), identity("us", 2));
        let other = identity("ap", 1);
        let now = Instant::now();
        for id in [&first, &second, &other] {
            staging.begin(&begin(id, "k"), now).unwrap();
        }
        stage(&staging, &first, 1, 0..600, now);
        stage(&staging, &other, 1, 0..1000, now);
        // In flight counts too.
        assert_eq!(
            staging.admit(&second, &data(1, 0..300), now).unwrap(),
            [0..300]
        );
        assert_eq!(
            staging.admit(&second, &data(1, 300..500), now),
            Err(AbortReason::QuotaExceeded)
        );
        // Only the staging that passed it is discarded.
        assert_eq!(staging.staged(&second), None);
        assert_eq!(staging.used(&first.cluster), 600);
        assert_eq!(staging.used(&other.cluster), 1000);
        // Its frame in flight settles into nothing.
        assert!(!staging.settle(&second, 1, 0..300, Some(extent(1, &(0..300)))));
        assert_eq!(staging.used(&first.cluster), 600);
        // Bytes the staging already holds cost nothing again.
        assert_eq!(staging.admit(&first, &data(1, 0..600), now).unwrap(), []);
        assert_eq!(
            staging.admit(&first, &data(1, 600..1000), now).unwrap(),
            [600..1000]
        );
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
        assert_eq!(staging.used(&idle.cluster), 10);
        // Sweeps run as staging is used, and DATA that finds its staging
        // gone is told it expired.
        let swept = later + LIMITS.ttl + SWEEP_INTERVAL;
        assert_eq!(
            staging.admit(&busy, &data(1, 10..20), swept),
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
            let parts = staging.admit(&id, &data(piece, 2 * at..2 * at + 1), now);
            assert_eq!(parts.unwrap().len(), 1);
        }
        let past = 2 * MAX_EXTENTS as u64;
        assert_eq!(
            staging.admit(&id, &data(piece, past..past + 1), now),
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
    /// gave each extent, and how often it stored each byte.
    struct Sink {
        body: Vec<u8>,
        stored: BTreeMap<EpochSeq, Vec<u8>>,
        copies: Vec<u8>,
        durable: ByteRanges,
    }

    impl Sink {
        /// Stores `range` and settles it as durable.
        fn land(&mut self, staging: &Staging, id: &WriteIdentity, range: Range<u64>) -> bool {
            let extent = extent(self.stored.len() as u64 + 1, &range);
            let (start, end) = (range.start as usize, range.end as usize);
            self.stored
                .insert(extent.position, self.body[start..end].to_vec());
            for copies in &mut self.copies[start..end] {
                *copies += 1;
            }
            self.durable.insert(range.clone());
            staging.settle(id, 0, range, Some(extent))
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
            let staging = Staging::new(StagingLimits { quota_bytes: len, ttl: LIMITS.ttl });
            let id = identity("us", 1);
            let now = Instant::now();
            let body: Vec<u8> = (0..len).map(|at| (at % 251) as u8).collect();
            let mut sink = Sink {
                body: body.clone(),
                stored: BTreeMap::new(),
                copies: vec![0; body.len()],
                durable: ByteRanges::new(),
            };
            let mut in_flight: Vec<Range<u64>> = Vec::new();
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
                        for part in staging.admit(&id, &frame, now).unwrap() {
                            let &(fails, stays) = outcomes.next().unwrap();
                            if stays {
                                in_flight.push(part);
                            } else if fails {
                                staging.settle(&id, 0, part, None);
                            } else {
                                proptest::prop_assert!(sink.land(&staging, &id, part));
                                let report = staging.report(&id, [0]).unwrap();
                                proptest::prop_assert_eq!(&report.pieces[&0], &sink.durable);
                            }
                        }
                    }
                }
                for range in landing {
                    sink.land(&staging, &id, range);
                }
            }
            proptest::prop_assert!(sink.copies.iter().all(|&copies| copies == 1));
            proptest::prop_assert_eq!(staging.used(&id.cluster), len);
            let extents = staging.staged(&id).unwrap().extents(0, len).unwrap();
            let assembled: Vec<u8> = extents
                .iter()
                .flat_map(|extent| sink.stored[&extent.position].clone())
                .collect();
            proptest::prop_assert_eq!(assembled, body);
        }
    }
}
