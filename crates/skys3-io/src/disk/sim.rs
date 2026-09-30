//! The simulated disk: files in memory, with crashes and injected faults.
//!
//! Each file keeps two images: what reads see (the page cache) and what
//! survives a crash (stable storage). The directory likewise keeps its current
//! entries and its durable entries. Syncs copy the first into the second, and
//! a crash throws away everything not copied, which is the worst case POSIX
//! allows, except where a torn write keeps part of it.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::{Disk, SegmentFile, check_name, read_past_end, truncate_extends};

/// Faults a [`SimDisk`] injects. Random choices come from the disk's seeded
/// generator, so a simulation seed replays them exactly.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimDiskFaults {
    /// The probability that a sync ([`SegmentFile::sync_data`] or
    /// [`Disk::sync_dir`]) fails with an I/O error. A failed data sync loses
    /// the file's unsynced bytes as Linux does: they stay readable until the
    /// crash, a later sync reports success, and after the crash the range
    /// reads as zeros.
    pub sync_error_probability: f64,

    /// The probability that a crash tears a file's unsynced writes instead of
    /// dropping them: a random prefix of the unsynced range, possibly all of
    /// it, reaches stable storage.
    pub torn_write_probability: f64,

    /// The disk's capacity in bytes, or `None` for unlimited. Appends past it
    /// write what fits and fail with [`io::ErrorKind::StorageFull`].
    pub capacity: Option<u64>,
}

/// Sizes of one file on a [`SimDisk`], for tests and checkers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimFileInfo {
    /// The file's length as reads see it.
    pub written: u64,
    /// The length of the prefix that syncs have covered. After a failed
    /// sync it includes bytes that were lost.
    pub synced: u64,
    /// Whether the file's directory entry is durable, so the file survives a
    /// crash.
    pub durable_entry: bool,
}

/// A simulated disk: the device, which outlives crashes of the node using it.
///
/// The node reaches the disk through a [`SimMount`] from [`SimDisk::mount`].
/// [`SimDisk::crash`] simulates power loss: it keeps only durable state and
/// makes every existing mount and file handle fail, and the restarted node
/// mounts the disk again. Clones share the same device.
#[derive(Clone)]
pub struct SimDisk {
    state: Arc<Mutex<DiskState>>,
}

type FileId = u64;

struct DiskState {
    /// Incremented by each crash. Mounts and handles from an earlier
    /// incarnation are stale.
    incarnation: u64,
    next_id: FileId,
    entries: BTreeMap<String, FileId>,
    durable_entries: BTreeMap<String, FileId>,
    files: BTreeMap<FileId, FileState>,
    /// Bytes held by all files, for the capacity limit.
    used: u64,
    faults: SimDiskFaults,
    forced_sync_failures: u32,
    rng: SmallRng,
    crashes: u64,
}

#[derive(Default)]
struct FileState {
    /// What reads see.
    data: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// The length of the prefix that syncs have covered: `durable[..synced]`
    /// is what stable storage holds for `data[..synced]`, which differs only
    /// where a failed sync lost bytes. `durable` is longer than `synced` only
    /// after an unsynced truncation.
    synced: usize,
    /// Open handles in the current incarnation.
    handles: usize,
}

impl FileState {
    fn with_contents(contents: Vec<u8>) -> Self {
        FileState {
            synced: contents.len(),
            data: contents.clone(),
            durable: contents,
            handles: 0,
        }
    }

    /// The bytes the file occupies on the device.
    fn footprint(&self) -> usize {
        self.data.len().max(self.durable.len())
    }

    fn sync(&mut self) {
        self.durable.truncate(self.synced);
        self.durable.extend_from_slice(&self.data[self.synced..]);
        self.synced = self.data.len();
    }

    /// A failed sync: the kernel drops the dirty range without writing it,
    /// but the file's length reaches stable storage.
    fn fail_sync(&mut self) {
        self.durable.truncate(self.synced);
        self.durable.resize(self.data.len(), 0);
        self.synced = self.data.len();
    }

    /// Stable storage after a crash that persists `torn` bytes of the
    /// unsynced range.
    fn crash_image(&self, torn: usize) -> Vec<u8> {
        let mut image = self.durable.clone();
        let torn_end = self.synced + torn;
        if image.len() < torn_end {
            image.resize(torn_end, 0);
        }
        image[self.synced..torn_end].copy_from_slice(&self.data[self.synced..torn_end]);
        image
    }
}

impl SimDisk {
    /// Returns an empty disk with no faults whose random choices are seeded
    /// by `seed`.
    pub fn new(seed: u64) -> Self {
        Self::with_faults(seed, SimDiskFaults::default())
    }

    /// Returns an empty disk that injects `faults`.
    pub fn with_faults(seed: u64, faults: SimDiskFaults) -> Self {
        SimDisk {
            state: Arc::new(Mutex::new(DiskState {
                incarnation: 0,
                next_id: 0,
                entries: BTreeMap::new(),
                durable_entries: BTreeMap::new(),
                files: BTreeMap::new(),
                used: 0,
                faults,
                forced_sync_failures: 0,
                rng: SmallRng::seed_from_u64(seed),
                crashes: 0,
            })),
        }
    }

    /// Returns a mount for the node that uses this disk until the next crash.
    pub fn mount(&self) -> SimMount {
        SimMount {
            disk: self.clone(),
            incarnation: self.lock().incarnation,
        }
    }

    /// Simulates a power loss: keeps only durable directory entries and
    /// durable file contents (plus torn writes, if configured) and makes
    /// every mount and file handle stale.
    pub fn crash(&self) {
        let mut state = self.lock();
        let state = &mut *state;
        let torn_probability = state.faults.torn_write_probability;
        let mut files = BTreeMap::new();
        let mut used = 0;
        for &id in state.durable_entries.values() {
            let file = &state.files[&id];
            let unsynced = file.data.len() - file.synced;
            let torn = if unsynced > 0 && chance(&mut state.rng, torn_probability) {
                state.rng.random_range(0..=unsynced)
            } else {
                0
            };
            let image = file.crash_image(torn);
            used += image.len() as u64;
            files.insert(id, FileState::with_contents(image));
        }
        state.files = files;
        state.used = used;
        state.entries = state.durable_entries.clone();
        state.incarnation += 1;
        state.crashes += 1;
    }

    /// Returns the number of crashes so far.
    pub fn crashes(&self) -> u64 {
        self.lock().crashes
    }

    /// Returns the faults the disk injects.
    pub fn faults(&self) -> SimDiskFaults {
        self.lock().faults.clone()
    }

    /// Replaces the faults the disk injects, for example to fill the disk
    /// partway through a simulation.
    pub fn set_faults(&self, faults: SimDiskFaults) {
        self.lock().faults = faults;
    }

    /// Makes the next `count` syncs fail, in addition to random failures.
    pub fn fail_next_syncs(&self, count: u32) {
        self.lock().forced_sync_failures += count;
    }

    /// Returns the bytes held by all files, as the capacity counts them.
    pub fn used_bytes(&self) -> u64 {
        self.lock().used
    }

    /// Returns the sizes of the file `name`, or `None` if it does not exist.
    pub fn file_info(&self, name: &str) -> Option<SimFileInfo> {
        let state = self.lock();
        let id = *state.entries.get(name)?;
        let file = &state.files[&id];
        Some(SimFileInfo {
            written: file.data.len() as u64,
            synced: file.synced as u64,
            durable_entry: state.durable_entries.get(name) == Some(&id),
        })
    }

    fn lock(&self) -> MutexGuard<'_, DiskState> {
        // A panic while the lock is held fails the simulation anyway.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the state, failing if the caller's incarnation has crashed.
    fn lock_live(&self, incarnation: u64) -> io::Result<MutexGuard<'_, DiskState>> {
        let state = self.lock();
        if state.incarnation == incarnation {
            Ok(state)
        } else {
            Err(io::Error::other(
                "simulated disk crashed; the handle is stale",
            ))
        }
    }
}

impl fmt::Debug for SimDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("SimDisk")
            .field("files", &state.entries.len())
            .field("used", &state.used)
            .field("crashes", &state.crashes)
            .field("faults", &state.faults)
            .finish()
    }
}

impl DiskState {
    /// Decides whether this sync fails, consuming a forced failure first.
    fn sync_fails(&mut self) -> bool {
        if self.forced_sync_failures > 0 {
            self.forced_sync_failures -= 1;
            return true;
        }
        chance(&mut self.rng, self.faults.sync_error_probability)
    }

    fn file(&mut self, id: FileId) -> &mut FileState {
        self.files
            .get_mut(&id)
            .expect("a live handle's file is kept until its last handle closes")
    }

    /// Applies `change` to a file and accounts for its change in size.
    fn update<R>(&mut self, id: FileId, change: impl FnOnce(&mut FileState) -> R) -> R {
        let file = self.file(id);
        let before = file.footprint() as u64;
        let result = change(file);
        let after = file.footprint() as u64;
        self.used = self.used - before + after;
        result
    }

    /// Drops a file that no entry or handle refers to any more.
    fn collect(&mut self, id: FileId) {
        let referenced = self.files[&id].handles > 0
            || self.entries.values().any(|&e| e == id)
            || self.durable_entries.values().any(|&e| e == id);
        if !referenced {
            let file = self.files.remove(&id).expect("file exists");
            self.used -= file.footprint() as u64;
        }
    }

    fn open_handle(&mut self, disk: &SimDisk, id: FileId) -> SimFile {
        self.file(id).handles += 1;
        SimFile {
            disk: disk.clone(),
            incarnation: self.incarnation,
            id,
        }
    }
}

fn chance(rng: &mut SmallRng, probability: f64) -> bool {
    // Draw only when needed, so a disk without faults consumes no randomness.
    probability > 0.0 && rng.random::<f64>() < probability
}

fn sync_error() -> io::Error {
    io::Error::other("injected sync error")
}

/// A node's view of a [`SimDisk`] until the next crash.
#[derive(Clone, Debug)]
pub struct SimMount {
    disk: SimDisk,
    incarnation: u64,
}

impl SimMount {
    /// Returns the underlying device.
    pub fn disk(&self) -> &SimDisk {
        &self.disk
    }
}

impl Disk for SimMount {
    type File = SimFile;

    async fn create(&self, name: &str) -> io::Result<SimFile> {
        check_name(name)?;
        let mut state = self.disk.lock_live(self.incarnation)?;
        if state.entries.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("segment file {name:?} exists"),
            ));
        }
        let id = state.next_id;
        state.next_id += 1;
        state.files.insert(id, FileState::default());
        state.entries.insert(name.to_owned(), id);
        Ok(state.open_handle(&self.disk, id))
    }

    async fn open(&self, name: &str) -> io::Result<SimFile> {
        check_name(name)?;
        let mut state = self.disk.lock_live(self.incarnation)?;
        let id = *state.entries.get(name).ok_or_else(|| not_found(name))?;
        Ok(state.open_handle(&self.disk, id))
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        check_name(name)?;
        let mut state = self.disk.lock_live(self.incarnation)?;
        let id = state.entries.remove(name).ok_or_else(|| not_found(name))?;
        state.collect(id);
        Ok(())
    }

    async fn list(&self) -> io::Result<Vec<String>> {
        let state = self.disk.lock_live(self.incarnation)?;
        Ok(state.entries.keys().cloned().collect())
    }

    async fn sync_dir(&self) -> io::Result<()> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        if state.sync_fails() {
            return Err(sync_error());
        }
        let entries = state.entries.clone();
        let replaced = std::mem::replace(&mut state.durable_entries, entries);
        for id in replaced.into_values() {
            if state.files.contains_key(&id) {
                state.collect(id);
            }
        }
        Ok(())
    }
}

fn not_found(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("segment file {name:?} does not exist"),
    )
}

/// An open file on a [`SimMount`].
pub struct SimFile {
    disk: SimDisk,
    incarnation: u64,
    id: FileId,
}

impl fmt::Debug for SimFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimFile")
            .field("id", &self.id)
            .field("incarnation", &self.incarnation)
            .finish()
    }
}

impl Drop for SimFile {
    fn drop(&mut self) {
        let mut state = self.disk.lock();
        if state.incarnation == self.incarnation {
            state.file(self.id).handles -= 1;
            state.collect(self.id);
        }
    }
}

impl SegmentFile for SimFile {
    /// Returns the file's length, or zero once the disk has crashed.
    fn len(&self) -> u64 {
        self.disk
            .lock_live(self.incarnation)
            .map_or(0, |mut state| state.file(self.id).data.len() as u64)
    }

    async fn append(&self, data: Bytes) -> io::Result<u64> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        let available = state
            .faults
            .capacity
            .map_or(u64::MAX, |capacity| capacity.saturating_sub(state.used));
        let fits = usize::try_from(available).map_or(data.len(), |a| a.min(data.len()));
        let offset = state.update(self.id, |file| {
            let offset = file.data.len() as u64;
            file.data.extend_from_slice(&data[..fits]);
            offset
        });
        if fits < data.len() {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated disk is full",
            ));
        }
        Ok(offset)
    }

    async fn sync_data(&self) -> io::Result<()> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        if state.sync_fails() {
            state.update(self.id, FileState::fail_sync);
            return Err(sync_error());
        }
        state.update(self.id, FileState::sync);
        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        let data = &state.file(self.id).data;
        let file_len = data.len() as u64;
        let start = usize::try_from(offset).ok().filter(|&s| s <= data.len());
        match start.and_then(|s| Some(s..s.checked_add(len)?)) {
            Some(range) if range.end <= data.len() => Ok(Bytes::copy_from_slice(&data[range])),
            _ => Err(read_past_end(offset, len, file_len)),
        }
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        state.update(self.id, |file| {
            let new_len = usize::try_from(len)
                .ok()
                .filter(|&l| l <= file.data.len())
                .ok_or_else(|| truncate_extends(len, file.data.len() as u64))?;
            file.data.truncate(new_len);
            file.synced = file.synced.min(new_len);
            Ok(())
        })
    }
}
