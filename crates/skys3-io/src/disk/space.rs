//! Space on a file system, as admission control (design §13) and the clean
//! cache's capacity model (§9.3) read it.

use std::io;
use std::path::Path;

/// The size of a file system and its free space, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    /// Every block of the file system, those reserved for the superuser
    /// included.
    pub total: u64,
    /// The bytes an unprivileged process can still write: space reserved
    /// for the superuser is not counted.
    pub available: u64,
}

/// The size and free space of the file system that holds `path`, as
/// `statvfs(3)` reports them: its blocks and available blocks times their
/// size.
///
/// The call blocks, so a node makes it on the disk's
/// [`BlockingPool`](crate::BlockingPool), never on the Tokio reactor
/// (design §10.4).
///
/// # Errors
///
/// The operating system's, such as [`io::ErrorKind::NotFound`] for a path
/// that does not exist.
///
/// ```
/// let space = skys3_io::disk::space(&std::env::temp_dir())?;
/// assert!(space.total >= space.available);
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn space(path: &Path) -> io::Result<Space> {
    let stat = rustix::fs::statvfs(path)?;
    Ok(Space {
        total: stat.f_blocks.saturating_mul(stat.f_frsize),
        available: stat.f_bavail.saturating_mul(stat.f_frsize),
    })
}

/// The bytes an unprivileged process can still write to the file system
/// that holds `path`: [`Space::available`] of [`space`].
///
/// # Errors
///
/// As [`space`].
///
/// ```
/// let dir = std::env::temp_dir();
/// assert!(skys3_io::disk::available_bytes(&dir)? > 0);
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn available_bytes(path: &Path) -> io::Result<u64> {
    space(path).map(|space| space.available)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_space_is_read_and_a_missing_path_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(available_bytes(dir.path()).unwrap() > 0);
        let space = space(dir.path()).unwrap();
        assert!(space.total >= space.available && space.total > 0);
        let error = available_bytes(&dir.path().join("missing")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
