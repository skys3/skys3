//! A power supply that simulated disks share, such as the disks of one
//! host, and that can fail at a planned sync.
//!
//! Crash-consistency tests enumerate sync boundaries (design §16.1): a run
//! counts the syncs of a host's disks, and each later run of the same seed
//! cuts the power at one of them, just before or just after it takes
//! effect. The cut happens inside the sync, so nothing the host does after
//! the boundary reaches stable storage, and every disk on the supply loses
//! power at the same moment.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use super::{DiskState, SimDisk};

/// Where a planned power cut falls relative to its sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SyncCut {
    /// The power fails as the sync starts: the writes it would have made
    /// durable are lost, except what a torn write keeps.
    Before,
    /// The power fails just after the sync made its writes durable, before
    /// the caller learns that it succeeded.
    After,
}

/// The power supply of one or more [`SimDisk`]s.
///
/// Disks join with [`SimDisk::set_power`]. The supply numbers every sync
/// any of them performs, in order and from 0: data syncs, directory syncs,
/// and block-file syncs, failed or not. [`SimPower::cut_at_sync`] plans a
/// power loss at one of them, which crashes every disk on the supply as
/// [`SimDisk::crash`] does. The sync itself then fails, as every later
/// operation of the handles it made stale does. Clones share the supply.
#[derive(Clone, Default)]
pub struct SimPower {
    state: Arc<Mutex<PowerState>>,
}

#[derive(Default)]
struct PowerState {
    syncs: u64,
    plan: Option<(u64, SyncCut)>,
    cut: bool,
    disks: Vec<Weak<Mutex<DiskState>>>,
}

impl SimPower {
    /// A supply with no disk and no planned cut.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The syncs of the supply's disks so far.
    #[must_use]
    pub fn syncs(&self) -> u64 {
        self.lock().syncs
    }

    /// Plans a power loss at the sync numbered `sync`, counting from 0 over
    /// every disk on the supply, replacing any earlier plan. A sync that
    /// has already happened is never reached, so planning one cuts nothing.
    pub fn cut_at_sync(&self, sync: u64, cut: SyncCut) {
        let mut state = self.lock();
        state.plan = Some((sync, cut));
        state.cut = false;
    }

    /// Whether the planned power loss has happened.
    #[must_use]
    pub fn is_cut(&self) -> bool {
        self.lock().cut
    }

    fn lock(&self) -> MutexGuard<'_, PowerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Puts `disk` on this supply.
    pub(super) fn attach(&self, disk: &Arc<Mutex<DiskState>>) {
        let mut state = self.lock();
        state.disks.retain(|weak| weak.strong_count() > 0);
        if !state
            .disks
            .iter()
            .any(|weak| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(disk)))
        {
            state.disks.push(Arc::downgrade(disk));
        }
    }

    /// Counts a sync, and returns where the power fails if it is the
    /// planned one.
    pub(super) fn count_sync(&self) -> Option<SyncCut> {
        let mut state = self.lock();
        let number = state.syncs;
        state.syncs += 1;
        match state.plan {
            Some((at, cut)) if at == number && !state.cut => {
                state.cut = true;
                Some(cut)
            }
            _ => None,
        }
    }

    /// Crashes every disk on the supply except `except`, which the caller
    /// crashed already while it held its lock.
    pub(super) fn cut_others(&self, except: &Arc<Mutex<DiskState>>) {
        let disks: Vec<Arc<Mutex<DiskState>>> =
            self.lock().disks.iter().filter_map(Weak::upgrade).collect();
        for disk in disks {
            if !Arc::ptr_eq(&disk, except) {
                SimDisk { state: disk }.crash();
            }
        }
    }
}

impl fmt::Debug for SimPower {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("SimPower")
            .field("syncs", &state.syncs)
            .field("plan", &state.plan)
            .field("cut", &state.cut)
            .field("disks", &state.disks.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{Disk, SegmentFile, SimDiskFaults};

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    /// Two disks on one supply, each with one durable file holding `a`.
    async fn two_disks(power: &SimPower) -> Vec<SimDisk> {
        let mut disks = Vec::new();
        for seed in 0..2 {
            let disk = SimDisk::new(seed);
            disk.set_power(power);
            disk.set_power(power);
            let mount = disk.mount();
            let file = mount.create("f").await.unwrap();
            file.append(Bytes::from_static(b"a")).await.unwrap();
            file.sync_data().await.unwrap();
            mount.sync_dir().await.unwrap();
            disks.push(disk);
        }
        disks
    }

    async fn contents(disk: &SimDisk) -> Vec<u8> {
        let file = disk.mount().open("f").await.unwrap();
        file.read_at(0, usize::try_from(file.len()).unwrap())
            .await
            .unwrap()
            .to_vec()
    }

    #[test]
    fn syncs_are_numbered_across_the_disks_of_a_supply() {
        runtime().block_on(async {
            let power = SimPower::new();
            let disks = two_disks(&power).await;
            assert_eq!(power.syncs(), 4);
            let block = disks[0].mount().open_block_file("index").unwrap();
            block.write_at(0, b"page").unwrap();
            block.sync().unwrap();
            assert_eq!(power.syncs(), 5);
            // A failed sync counts too.
            disks[1].fail_next_syncs(1);
            disks[1].mount().sync_dir().await.unwrap_err();
            assert_eq!(power.syncs(), 6);
            assert!(!power.is_cut());
            let debug = format!("{power:?}");
            assert!(
                debug.contains("syncs: 6") && debug.contains("disks: 2"),
                "{debug}"
            );
        });
    }

    #[test]
    fn a_cut_before_a_sync_loses_its_writes_on_every_disk() {
        runtime().block_on(async {
            let power = SimPower::new();
            let disks = two_disks(&power).await;
            let first = disks[0].mount().open("f").await.unwrap();
            let second = disks[1].mount().open("f").await.unwrap();
            first.append(Bytes::from_static(b"b")).await.unwrap();
            second.append(Bytes::from_static(b"b")).await.unwrap();
            power.cut_at_sync(power.syncs(), SyncCut::Before);
            first.sync_data().await.unwrap_err();
            assert!(power.is_cut());
            // Both disks lost power: their handles are stale and only the
            // synced bytes remain.
            second.sync_data().await.unwrap_err();
            for disk in &disks {
                assert_eq!(disk.crashes(), 1);
                assert_eq!(contents(disk).await, b"a");
            }
        });
    }

    #[test]
    fn a_cut_after_a_sync_keeps_its_writes_but_fails_it() {
        runtime().block_on(async {
            let power = SimPower::new();
            let disks = two_disks(&power).await;
            let first = disks[0].mount().open("f").await.unwrap();
            let second = disks[1].mount().open("f").await.unwrap();
            first.append(Bytes::from_static(b"b")).await.unwrap();
            second.append(Bytes::from_static(b"b")).await.unwrap();
            power.cut_at_sync(power.syncs(), SyncCut::After);
            first.sync_data().await.unwrap_err();
            assert_eq!(contents(&disks[0]).await, b"ab");
            assert_eq!(contents(&disks[1]).await, b"a");
            // The plan fires once; later syncs pass.
            let file = disks[0].mount().open("f").await.unwrap();
            file.append(Bytes::from_static(b"c")).await.unwrap();
            file.sync_data().await.unwrap();
            assert_eq!(disks[0].crashes(), 1);
        });
    }

    #[test]
    fn cuts_fall_on_directory_and_block_file_syncs() {
        runtime().block_on(async {
            let power = SimPower::new();
            let disk = SimDisk::with_faults(
                3,
                SimDiskFaults {
                    torn_write_probability: 0.0,
                    ..SimDiskFaults::default()
                },
            );
            disk.set_power(&power);
            let mount = disk.mount();
            mount.create("new").await.unwrap();
            power.cut_at_sync(0, SyncCut::Before);
            mount.sync_dir().await.unwrap_err();
            assert!(disk.mount().list().await.unwrap().is_empty());

            let block = disk.mount().open_block_file("index").unwrap();
            block.write_at(0, b"page").unwrap();
            power.cut_at_sync(1, SyncCut::After);
            block.sync().unwrap_err();
            let block = disk.mount().open_block_file("index").unwrap();
            assert_eq!(block.len().unwrap(), 4);
            assert_eq!(disk.crashes(), 2);

            // A plan for a sync already past never fires.
            power.cut_at_sync(0, SyncCut::Before);
            disk.mount().sync_dir().await.unwrap();
            assert!(!power.is_cut());
        });
    }

    #[test]
    fn dropped_disks_leave_the_supply() {
        let power = SimPower::new();
        let kept = SimDisk::new(1);
        kept.set_power(&power);
        drop({
            let gone = SimDisk::new(2);
            gone.set_power(&power);
            gone
        });
        SimDisk::new(3).set_power(&power);
        // Each attach forgets disks that are gone.
        assert_eq!(power.lock().disks.len(), 2);
        let state = Arc::clone(&kept.state);
        power.cut_others(&state);
        assert_eq!(kept.crashes(), 0);
    }
}
