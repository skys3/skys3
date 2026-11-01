//! Crash simulation of fragment-segment compaction (plan M5-07, §10.3).
//!
//! Each seed draws a workload of fragments large enough to fill several
//! segments, reclaims about three in four, and compacts at a threshold
//! of one half or one. A first run counts the syncs; then one run per sync
//! of the reclaims and the compaction cuts the power just before or just
//! after it (torn writes on half of the crashes), and one more per sync
//! kills the process there. Each time, a second life must find every
//! fragment that was not reclaimed, whole and under its ID, compact again
//! to segments that hold nothing dead, and keep it all through another
//! power cut.

use skys3_ec::CompactionReport;
use skys3_io::Disk;

use super::{Acked, Acks, SIZES, TORN, Workload, check, open, recover_and_check, write_all};
use crate::support::KillMount;
use rand::Rng;
use skys3_ec::{FragmentStore, SeededBug};
use skys3_io::{SimDisk, SimPower, SyncCut};
use skys3_sim::SimContext;

/// A seed's compaction.
#[derive(Clone, Debug)]
pub(crate) struct Compaction {
    workload: Workload,
    /// Mixed into each fragment's key to choose the fragments reclaimed.
    choice: u64,
    /// The live share below which a segment is compacted.
    threshold: f64,
}

/// Where a run crashes, counted over all of its syncs.
#[derive(Clone, Copy, Debug)]
enum Crash {
    Power(u64, SyncCut),
    Kill(u64),
}

/// What the first life of a run did.
struct FirstLife {
    /// The fragments not reclaimed.
    live: Vec<Acked>,
    /// The syncs before the first reclaim.
    base: u64,
    /// The compaction's outcome, if it ran to the end.
    report: Option<CompactionReport>,
}

impl Compaction {
    pub(crate) fn draw(context: &mut SimContext) -> Self {
        let disk_seed = context.fork_seed();
        let writers = context.rng().random_range(2..=4);
        // Fragments of 64 KiB and more: three or four writers' worth fill
        // several 256 KiB segments.
        let large = &SIZES[3..];
        let sizes = (0..writers)
            .map(|_| {
                let count = context.rng().random_range(3..=6);
                (0..count)
                    .map(|_| large[context.rng().random_range(0..large.len())])
                    .collect()
            })
            .collect();
        let choice = context.rng().random();
        let threshold = if context.rng().random_bool(0.5) {
            0.5
        } else {
            1.0
        };
        Self {
            workload: Workload {
                disk_seed,
                writers,
                sizes,
            },
            choice,
            threshold,
        }
    }

    /// Whether the run reclaims `acked`: three in four, by key.
    fn reclaims(&self, acked: &Acked) -> bool {
        let hash = acked.header.key.bytes().fold(self.choice, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
        hash % 4 != 0
    }

    /// Writes the workload, reclaims, and compacts on `disk`, counting
    /// syncs with `syncs`.
    async fn first_life<D: Disk>(
        &self,
        disk: D,
        bug: Option<SeededBug>,
        syncs: impl Fn() -> u64,
    ) -> Result<FirstLife, String> {
        let store = open(disk, bug)
            .await
            .ok_or("the first life failed to open")?;
        let acks = Acks::default();
        write_all(&store, &self.workload, 0..self.workload.writers, &acks).await;
        let acks = acks.lock().unwrap().clone();
        let base = syncs();
        let (gone, live): (Vec<Acked>, Vec<Acked>) =
            acks.into_iter().partition(|acked| self.reclaims(acked));
        for acked in &gone {
            if store.reclaim(acked.id).await.is_err() {
                break;
            }
        }
        let report = store.compact(self.threshold).await.ok();
        if report.is_some() {
            // Nothing was lost along the way, crash or no crash.
            check(&store, &live).await?;
        }
        Ok(FirstLife { live, base, report })
    }

    /// The second life, after the crash: every live fragment is there,
    /// and a compaction at threshold one leaves no dead byte outside the
    /// last segment.
    async fn second_life(&self, disk: &SimDisk, live: Vec<Acked>) -> Result<(), String> {
        let (store, _) = FragmentStore::open(disk.mount(), crate::support::small_config())
            .await
            .map_err(|error| format!("recovery refused to start: {error}"))?;
        check(&store, &live).await?;
        store
            .compact(1.0)
            .await
            .map_err(|error| format!("the second life failed to compact: {error}"))?;
        check(&store, &live).await?;
        let segments = store.segments();
        let sealed: u64 = segments
            .iter()
            .take(segments.len().saturating_sub(1))
            .map(|segment| segment.len)
            .sum();
        if sealed > store.live_bytes() {
            return Err(format!(
                "{sealed} bytes of sealed segments for {} live bytes",
                store.live_bytes()
            ));
        }
        drop(store);
        disk.crash();
        recover_and_check(disk, live).await
    }

    /// Runs the compaction, crashing at `crash` if any, then lives on.
    /// Returns the first life's syncs before the first reclaim, its syncs
    /// in all, and its compaction report.
    fn run(
        &self,
        crash: Option<Crash>,
        bug: Option<SeededBug>,
    ) -> Result<(u64, u64, Option<CompactionReport>), String> {
        crate::support::runtime().block_on(async {
            let disk = SimDisk::with_faults(self.workload.disk_seed, TORN);
            let (first, syncs) = match crash {
                Some(Crash::Kill(at)) => {
                    let mount = KillMount::new(&disk, Some(at));
                    let counter = mount.clone();
                    let first = self.first_life(mount, bug, || counter.syncs()).await?;
                    let syncs = counter.syncs();
                    disk.kill();
                    (first, syncs)
                }
                _ => {
                    let power = SimPower::new();
                    disk.set_power(&power);
                    if let Some(Crash::Power(at, side)) = crash {
                        power.cut_at_sync(at, side);
                    }
                    let first = self.first_life(disk.mount(), bug, || power.syncs()).await?;
                    let syncs = power.syncs();
                    power.cut_at_sync(u64::MAX, SyncCut::Before);
                    disk.crash();
                    (first, syncs)
                }
            };
            self.second_life(&disk, first.live).await?;
            Ok((first.base, syncs, first.report))
        })
    }
}

/// Cuts the power before and after every sync of `context`'s compaction,
/// and kills the process at each.
pub(crate) fn crashes(context: &mut SimContext, bug: Option<SeededBug>) -> Result<(), String> {
    let compaction = Compaction::draw(context);
    let (base, syncs, report) = compaction.run(None, bug)?;
    if report.is_none() {
        return Err("the compaction failed without a crash".to_owned());
    }
    for at in base..syncs {
        for side in [SyncCut::Before, SyncCut::After] {
            compaction
                .run(Some(Crash::Power(at, side)), bug)
                .map_err(|error| format!("power cut {side:?} sync {at}: {error}"))?;
        }
        compaction
            .run(Some(Crash::Kill(at)), bug)
            .map_err(|error| format!("process crash at sync {at}: {error}"))?;
    }
    Ok(())
}

/// The compaction of seed `seed` without a crash: its syncs and report.
pub(crate) fn uncut(seed: u64) -> (u64, u64, Option<CompactionReport>) {
    Compaction::draw(&mut SimContext::new(seed))
        .run(None, None)
        .unwrap()
}
