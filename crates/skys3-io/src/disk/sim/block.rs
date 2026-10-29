//! Random-access block files on a simulated disk.
//!
//! An embedded database such as the node's index (design §10.2) rewrites
//! pages in place, which segment files cannot express. A block file keeps
//! what reads see, what survives a crash, and the writes in between, in
//! order. A sync makes them all durable. A crash keeps the durable image,
//! plus each later write with the disk's torn-write probability, as a random
//! prefix: any subset of unsynced writes may reach stable storage, as on a
//! real disk that reorders its write-back.

use std::collections::BTreeMap;
use std::fmt;
use std::io;

use rand::Rng;
use rand::rngs::SmallRng;

use super::{SimDisk, SimMount, chance, check_name, sync_error};

/// One write since the last sync.
#[derive(Debug)]
enum Unsynced {
    Write { offset: usize, data: Vec<u8> },
    SetLen(usize),
}

/// The state of one block file.
#[derive(Debug, Default)]
pub(super) struct BlockState {
    /// What reads see.
    data: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// Writes since the last sync, oldest first.
    unsynced: Vec<Unsynced>,
    /// Whether a sync has made the file's directory entry durable.
    durable_entry: bool,
    /// Whether a sync failed. The writes it covered are lost, and the file
    /// refuses every write and sync until the next crash.
    failed: bool,
}

impl BlockState {
    fn set_len(image: &mut Vec<u8>, len: usize) {
        image.resize(len, 0);
    }

    fn write(image: &mut Vec<u8>, offset: usize, data: &[u8]) {
        let end = offset + data.len();
        if image.len() < end {
            image.resize(end, 0);
        }
        image[offset..end].copy_from_slice(data);
    }
}

/// Applies a crash to every block file: files without a durable entry
/// vanish, and the others keep their durable image plus whichever unsynced
/// writes the crash lets through.
pub(super) fn crash(
    blocks: &mut BTreeMap<String, BlockState>,
    rng: &mut SmallRng,
    torn_probability: f64,
) {
    blocks.retain(|_, block| block.durable_entry);
    for block in blocks.values_mut() {
        let mut image = std::mem::take(&mut block.durable);
        if !block.failed {
            for write in &block.unsynced {
                if !chance(rng, torn_probability) {
                    continue;
                }
                match write {
                    Unsynced::Write { offset, data } => {
                        let kept = rng.random_range(0..=data.len());
                        BlockState::write(&mut image, *offset, &data[..kept]);
                    }
                    Unsynced::SetLen(len) => BlockState::set_len(&mut image, *len),
                }
            }
        }
        *block = BlockState {
            data: image.clone(),
            durable: image,
            unsynced: Vec::new(),
            durable_entry: true,
            failed: false,
        };
    }
}

impl SimMount {
    /// Opens the block file `name`, creating it empty if it does not exist.
    ///
    /// Block files live in their own namespace on the disk: [`Disk::list`]
    /// does not show them, and a segment file may share a name with one. A
    /// new block file survives a crash only after a successful
    /// [`SimBlockFile::sync`], which makes its directory entry durable too.
    /// Block files do not count against the disk's capacity.
    ///
    /// [`Disk::list`]: crate::Disk::list
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if the name is not valid, and an
    /// error once the disk has crashed.
    pub fn open_block_file(&self, name: &str) -> io::Result<SimBlockFile> {
        check_name(name)?;
        let mut state = self.disk.lock_live(self.incarnation)?;
        state.blocks.entry(name.to_owned()).or_default();
        Ok(SimBlockFile {
            disk: self.disk.clone(),
            incarnation: self.incarnation,
            name: name.to_owned(),
        })
    }
}

/// A random-access file on a [`SimDisk`], for an embedded database.
///
/// Its operations are synchronous, as a database's storage interface
/// expects, and finish at once. Writes are visible to reads immediately and
/// durable only after a successful [`SimBlockFile::sync`]. After a failed
/// sync, the writes it covered are lost and the file refuses every write and
/// sync until the disk crashes: a database treats such an error as fatal,
/// and nothing written after it may be trusted.
pub struct SimBlockFile {
    disk: SimDisk,
    incarnation: u64,
    name: String,
}

impl fmt::Debug for SimBlockFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimBlockFile")
            .field("name", &self.name)
            .field("incarnation", &self.incarnation)
            .finish()
    }
}

impl SimBlockFile {
    /// Runs `op` on the file's state, failing once the disk has crashed.
    fn with<R>(&self, op: impl FnOnce(&mut super::DiskState) -> io::Result<R>) -> io::Result<R> {
        let mut state = self.disk.lock_live(self.incarnation)?;
        op(&mut state)
    }

    fn block<'a>(&self, state: &'a mut super::DiskState) -> &'a mut BlockState {
        state
            .blocks
            .get_mut(&self.name)
            .expect("a block file is kept until the next crash")
    }

    fn writable<'a>(&self, state: &'a mut super::DiskState) -> io::Result<&'a mut BlockState> {
        let block = self.block(state);
        if block.failed {
            return Err(io::Error::other(
                "a sync of this block file failed; it is out of service until the disk restarts",
            ));
        }
        Ok(block)
    }

    /// Returns the file's length.
    ///
    /// # Errors
    ///
    /// Fails once the disk has crashed.
    #[allow(clippy::len_without_is_empty, reason = "the length is fallible")]
    pub fn len(&self) -> io::Result<u64> {
        self.with(|state| Ok(self.block(state).data.len() as u64))
    }

    /// Fills `out` with the bytes at `offset`.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::UnexpectedEof`] if the range extends past the end of
    /// the file, and an error once the disk has crashed.
    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.with(|state| {
            let data = &self.block(state).data;
            let range = usize::try_from(offset)
                .ok()
                .and_then(|start| Some(start..start.checked_add(out.len())?))
                .filter(|range| range.end <= data.len())
                .ok_or_else(|| super::read_past_end(offset, out.len(), data.len() as u64))?;
            out.copy_from_slice(&data[range]);
            Ok(())
        })
    }

    /// Writes `data` at `offset`, extending the file with zeros if the
    /// offset is past its end.
    ///
    /// # Errors
    ///
    /// Fails after a failed sync and once the disk has crashed.
    pub fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let offset = to_usize(offset)?;
        offset
            .checked_add(data.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write past the end"))?;
        self.with(|state| {
            let block = self.writable(state)?;
            BlockState::write(&mut block.data, offset, data);
            block.unsynced.push(Unsynced::Write {
                offset,
                data: data.to_vec(),
            });
            Ok(())
        })
    }

    /// Sets the file's length, cutting it or extending it with zeros.
    ///
    /// # Errors
    ///
    /// Fails after a failed sync and once the disk has crashed.
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        let len = to_usize(len)?;
        self.with(|state| {
            let block = self.writable(state)?;
            BlockState::set_len(&mut block.data, len);
            block.unsynced.push(Unsynced::SetLen(len));
            Ok(())
        })
    }

    /// Makes every earlier write and the file's directory entry durable,
    /// like `fsync` of the file and of its directory.
    ///
    /// # Errors
    ///
    /// An injected sync error, after which the file refuses further writes,
    /// and an error once the disk has crashed.
    pub fn sync(&self) -> io::Result<()> {
        self.with(|state| {
            let fails = state.sync_fails();
            let block = self.writable(state)?;
            if fails {
                block.failed = true;
                block.unsynced.clear();
                return Err(sync_error());
            }
            block.durable.clone_from(&block.data);
            block.unsynced.clear();
            block.durable_entry = true;
            Ok(())
        })
    }
}

fn to_usize(value: u64) -> io::Result<usize> {
    usize::try_from(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset out of range"))
}

#[cfg(test)]
mod tests {
    use crate::{SimDisk, SimDiskFaults};

    fn contents(file: &super::SimBlockFile) -> Vec<u8> {
        let mut out = vec![0; usize::try_from(file.len().unwrap()).unwrap()];
        file.read_at(0, &mut out).unwrap();
        out
    }

    #[test]
    fn writes_survive_a_crash_only_once_synced() {
        let disk = SimDisk::new(1);
        let file = disk.mount().open_block_file("index").unwrap();
        file.write_at(0, b"hello").unwrap();
        // Never synced: the file itself is gone.
        disk.crash();
        assert!(file.len().is_err());
        let file = disk.mount().open_block_file("index").unwrap();
        assert_eq!(file.len().unwrap(), 0);

        file.write_at(2, b"abc").unwrap();
        assert_eq!(contents(&file), b"\0\0abc");
        file.sync().unwrap();
        file.write_at(0, b"zz").unwrap();
        file.set_len(4).unwrap();
        assert_eq!(contents(&file), b"zzab");
        disk.crash();
        let file = disk.mount().open_block_file("index").unwrap();
        assert_eq!(contents(&file), b"\0\0abc");
        let mut out = [0; 2];
        assert_eq!(
            file.read_at(4, &mut out).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert!(file.write_at(u64::MAX, b"x").is_err());
        assert!(disk.mount().open_block_file("a/b").is_err());
        assert!(format!("{file:?}").contains("index"));
    }

    #[test]
    fn a_crash_may_keep_any_subset_of_unsynced_writes() {
        let faults = SimDiskFaults {
            torn_write_probability: 1.0,
            ..SimDiskFaults::default()
        };
        let disk = SimDisk::with_faults(3, faults);
        let file = disk.mount().open_block_file("index").unwrap();
        file.write_at(0, b"0000").unwrap();
        file.sync().unwrap();
        file.write_at(0, b"1111").unwrap();
        file.set_len(6).unwrap();
        disk.crash();
        let file = disk.mount().open_block_file("index").unwrap();
        let image = contents(&file);
        // Every write went through, each as some prefix.
        assert_eq!(image.len(), 6);
        assert!(
            image[..4].iter().all(|&b| b == b'0' || b == b'1'),
            "{image:?}"
        );
    }

    #[test]
    fn a_failed_sync_loses_its_writes_and_takes_the_file_out_of_service() {
        let faults = SimDiskFaults {
            torn_write_probability: 1.0,
            ..SimDiskFaults::default()
        };
        let disk = SimDisk::with_faults(5, faults);
        let file = disk.mount().open_block_file("index").unwrap();
        file.write_at(0, b"keep").unwrap();
        file.sync().unwrap();
        file.write_at(0, b"lost").unwrap();
        disk.fail_next_syncs(1);
        assert!(file.sync().is_err());
        assert!(file.write_at(0, b"more").is_err());
        assert!(file.set_len(0).is_err());
        assert!(file.sync().is_err());
        // Reads still see the page cache.
        assert_eq!(contents(&file), b"lost");
        disk.crash();
        let file = disk.mount().open_block_file("index").unwrap();
        assert_eq!(contents(&file), b"keep");
        file.write_at(0, b"next").unwrap();
        file.sync().unwrap();
    }
}
