//! Free space on a file system, as admission control reads it (design
//! §13).

use std::io;
use std::path::Path;

/// The bytes an unprivileged process can still write to the file system
/// that holds `path`: its available blocks times their size, as
/// `statvfs(3)` reports them. Space reserved for the superuser is not
/// counted.
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
/// let dir = std::env::temp_dir();
/// assert!(skys3_io::disk::available_bytes(&dir)? > 0);
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn available_bytes(path: &Path) -> io::Result<u64> {
    let stat = rustix::fs::statvfs(path)?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_space_is_read_and_a_missing_path_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(available_bytes(dir.path()).unwrap() > 0);
        let error = available_bytes(&dir.path().join("missing")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
