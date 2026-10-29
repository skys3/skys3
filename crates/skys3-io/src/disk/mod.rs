//! Disks and append-only segment files.
//!
//! A [`Disk`] is one directory of segment files on one physical disk. The
//! log (design §10.1) appends records to a [`SegmentFile`], makes them durable
//! with [`SegmentFile::sync_data`] (`fdatasync`), and makes new files durable
//! with [`Disk::sync_dir`] (`fsync` of the directory) before it acknowledges
//! anything they hold (design §10.4).
//!
//! Durability follows POSIX, and the simulated disk enforces the worst case:
//!
//! - Appended bytes are visible to reads at once but survive a crash only
//!   once a later `sync_data` of that file succeeds.
//! - A created or removed file survives a crash as created or removed only
//!   once a later `sync_dir` succeeds, even if its data was synced.
//! - After a failed sync, the unsynced bytes may be lost even if a later sync
//!   succeeds, as on Linux. A caller must not acknowledge anything after a
//!   failed sync; the log takes the disk out of service instead.
//!
//! [`RealDisk`] runs every operation on a [`BlockingPool`](crate::BlockingPool).
//! [`SimDisk`] keeps files in memory and injects crashes and faults.

mod real;
mod sim;

use std::fmt;
use std::future::Future;
use std::io;

use bytes::Bytes;

pub use real::{RealDisk, RealFile};
pub use sim::{SimBlockFile, SimDisk, SimDiskFaults, SimFile, SimFileInfo, SimMount};

/// The longest file name a disk accepts, in bytes.
pub const MAX_NAME_LEN: usize = 255;

/// A directory of segment files on one disk.
///
/// File names are single path components: not empty, at most
/// [`MAX_NAME_LEN`] bytes, without `/` or NUL, and neither `.` nor `..`.
pub trait Disk: fmt::Debug + Send + Sync + 'static {
    /// The file type this disk opens.
    type File: SegmentFile;

    /// Creates the empty file `name` and opens it for appending and reading.
    ///
    /// The file survives a crash only after a later [`Disk::sync_dir`].
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::AlreadyExists`] if the file exists, and
    /// [`io::ErrorKind::InvalidInput`] if the name is not valid.
    fn create(&self, name: &str) -> impl Future<Output = io::Result<Self::File>> + Send;

    /// Opens the existing file `name` for appending and reading.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] if the file does not exist, and
    /// [`io::ErrorKind::InvalidInput`] if the name is not valid.
    fn open(&self, name: &str) -> impl Future<Output = io::Result<Self::File>> + Send;

    /// Removes the file `name`. Open handles to it keep working.
    ///
    /// The removal survives a crash only after a later [`Disk::sync_dir`].
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] if the file does not exist, and
    /// [`io::ErrorKind::InvalidInput`] if the name is not valid.
    fn remove(&self, name: &str) -> impl Future<Output = io::Result<()>> + Send;

    /// Returns the names of the files on the disk, sorted.
    fn list(&self) -> impl Future<Output = io::Result<Vec<String>>> + Send;

    /// Makes every earlier create and remove durable (`fsync` of the
    /// directory).
    fn sync_dir(&self) -> impl Future<Output = io::Result<()>> + Send;
}

/// An open segment file: appended at the end, read at any offset.
///
/// Appends to one file are applied one at a time. Concurrent appends land in
/// an unspecified order, so a writer that needs an order awaits each append.
pub trait SegmentFile: fmt::Debug + Send + Sync + 'static {
    /// Returns the file's length: everything appended so far, synced or not.
    fn len(&self) -> u64;

    /// Returns whether the file is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends `data` at the end of the file and returns the offset it was
    /// written at.
    ///
    /// # Errors
    ///
    /// On an error, a prefix of `data` may have been written, as with a
    /// short write on a real disk; [`SegmentFile::len`] reports the file's
    /// actual length. A full disk returns [`io::ErrorKind::StorageFull`].
    fn append(&self, data: Bytes) -> impl Future<Output = io::Result<u64>> + Send;

    /// Makes the file's contents and length durable (`fdatasync`).
    ///
    /// # Errors
    ///
    /// After an error, bytes appended since the last successful sync may be
    /// lost in a crash, even if a later sync succeeds.
    fn sync_data(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Reads `len` bytes at `offset`.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::UnexpectedEof`] if the range extends past the end of
    /// the file.
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;

    /// Cuts the file to `len` bytes, as recovery does to a torn tail. The new
    /// length is durable only after a later [`SegmentFile::sync_data`].
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `len` exceeds the file's length.
    fn truncate(&self, len: u64) -> impl Future<Output = io::Result<()>> + Send;
}

/// Checks that `name` is a valid file name for a [`Disk`].
pub(crate) fn check_name(name: &str) -> io::Result<()> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != "."
        && name != ".."
        && !name.contains(['/', '\0']);
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid segment file name {name:?}"),
        ))
    }
}

/// The error for a read past the end of a file.
pub(crate) fn read_past_end(offset: u64, len: usize, file_len: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("read of {len} bytes at offset {offset} past the end of a {file_len}-byte file"),
    )
}

/// The error for a truncation that would extend a file.
pub(crate) fn truncate_extends(len: u64, file_len: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("cannot truncate a {file_len}-byte file to {len} bytes"),
    )
}

#[cfg(test)]
mod tests;
