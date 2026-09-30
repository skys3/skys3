//! The real disk: a directory on the local file system, with every system
//! call on a dedicated blocking pool.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use bytes::{Bytes, BytesMut};

use super::{Disk, SegmentFile, check_name, read_past_end, truncate_extends};
use crate::pool::BlockingPool;

/// A directory of segment files on a local disk.
///
/// Every operation runs on the disk's [`BlockingPool`], never on the calling
/// task's thread. Give each physical disk its own pool.
///
/// Opening a file that is already open returns a handle to the same open
/// file, so every handle sees the length the others appended to.
#[derive(Clone, Debug)]
pub struct RealDisk {
    dir: Arc<Path>,
    pool: BlockingPool,
    open_files: Arc<Mutex<HashMap<String, Weak<FileInner>>>>,
}

impl RealDisk {
    /// Opens the segment directory `dir`, creating it and its parents if
    /// needed, with I/O on `pool`.
    ///
    /// Creating the directory is not made durable here; call
    /// [`Disk::sync_dir`] on the parent's disk if that matters.
    ///
    /// # Errors
    ///
    /// Returns the error from creating the directory, or an error if `dir`
    /// exists but is not a directory.
    pub async fn open(dir: impl Into<PathBuf>, pool: BlockingPool) -> io::Result<Self> {
        let dir: PathBuf = dir.into();
        let dir = pool
            .run(move || -> io::Result<PathBuf> {
                fs::create_dir_all(&dir)?;
                Ok(dir)
            })
            .await??;
        Ok(RealDisk {
            dir: dir.into(),
            pool,
            open_files: Arc::default(),
        })
    }

    /// Returns the segment directory.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Returns the pool this disk runs its I/O on.
    pub fn pool(&self) -> &BlockingPool {
        &self.pool
    }

    fn file_path(&self, name: &str) -> io::Result<PathBuf> {
        check_name(name)?;
        Ok(self.dir.join(name))
    }

    /// Opens `name` on the pool, or with `create`, creates it, and registers
    /// the open file so later opens share it.
    async fn open_file(&self, name: &str, create: bool) -> io::Result<RealFile> {
        let path = self.file_path(name)?;
        if !create && let Some(inner) = self.open_files().get(name).and_then(Weak::upgrade) {
            return Ok(self.handle(inner));
        }
        let (file, len) = self
            .pool
            .run(move || -> io::Result<(File, u64)> {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(create)
                    .open(path)?;
                let len = file.metadata()?.len();
                Ok((file, len))
            })
            .await??;
        let mut open_files = self.open_files();
        // Another task may have opened the file meanwhile: share it.
        if !create && let Some(inner) = open_files.get(name).and_then(Weak::upgrade) {
            return Ok(self.handle(inner));
        }
        let inner = Arc::new(FileInner {
            file,
            write_lock: Mutex::new(()),
            len: AtomicU64::new(len),
        });
        open_files.retain(|_, file| file.strong_count() > 0);
        open_files.insert(name.to_owned(), Arc::downgrade(&inner));
        Ok(self.handle(inner))
    }

    fn handle(&self, inner: Arc<FileInner>) -> RealFile {
        RealFile {
            inner,
            pool: self.pool.clone(),
        }
    }

    fn open_files(&self) -> MutexGuard<'_, HashMap<String, Weak<FileInner>>> {
        self.open_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Disk for RealDisk {
    type File = RealFile;

    async fn create(&self, name: &str) -> io::Result<RealFile> {
        self.open_file(name, true).await
    }

    async fn open(&self, name: &str) -> io::Result<RealFile> {
        self.open_file(name, false).await
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        let path = self.file_path(name)?;
        self.pool.run(move || fs::remove_file(path)).await??;
        // Open handles keep the unlinked file; a new file may take the name.
        self.open_files().remove(name);
        Ok(())
    }

    async fn list(&self) -> io::Result<Vec<String>> {
        let dir = Arc::clone(&self.dir);
        self.pool
            .run(move || -> io::Result<Vec<String>> {
                let mut names = Vec::new();
                for entry in fs::read_dir(&dir)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    // Names that are not UTF-8 were not created through a Disk.
                    if let Ok(name) = entry.file_name().into_string() {
                        names.push(name);
                    }
                }
                names.sort_unstable();
                Ok(names)
            })
            .await?
    }

    async fn sync_dir(&self) -> io::Result<()> {
        let dir = Arc::clone(&self.dir);
        self.pool.run(move || File::open(&dir)?.sync_all()).await?
    }
}

/// A segment file on a [`RealDisk`].
///
/// Cloning returns another handle to the same open file.
#[derive(Clone, Debug)]
pub struct RealFile {
    inner: Arc<FileInner>,
    pool: BlockingPool,
}

#[derive(Debug)]
struct FileInner {
    file: File,
    /// Serializes appends and truncations. Locked only on pool threads.
    write_lock: Mutex<()>,
    /// The file's length, published after each append or truncation.
    len: AtomicU64,
}

impl FileInner {
    fn append(&self, data: &[u8]) -> io::Result<u64> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let offset = self.len.load(Ordering::Acquire);
        let written = self.file.write_all_at(data, offset);
        if written.is_err() {
            // A prefix may have been written: report the actual length.
            if let Ok(metadata) = self.file.metadata() {
                self.len.store(metadata.len(), Ordering::Release);
            }
        } else {
            self.len
                .store(offset + data.len() as u64, Ordering::Release);
        }
        written.map(|()| offset)
    }

    fn truncate(&self, len: u64) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let file_len = self.len.load(Ordering::Acquire);
        if len > file_len {
            return Err(truncate_extends(len, file_len));
        }
        self.file.set_len(len)?;
        self.len.store(len, Ordering::Release);
        Ok(())
    }

    fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let file_len = self.len.load(Ordering::Acquire);
        let in_bounds = offset
            .checked_add(len as u64)
            .is_some_and(|end| end <= file_len);
        if !in_bounds {
            return Err(read_past_end(offset, len, file_len));
        }
        let mut buf = BytesMut::zeroed(len);
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(buf.freeze())
    }
}

impl SegmentFile for RealFile {
    fn len(&self) -> u64 {
        self.inner.len.load(Ordering::Acquire)
    }

    async fn append(&self, data: Bytes) -> io::Result<u64> {
        let inner = Arc::clone(&self.inner);
        self.pool.run(move || inner.append(&data)).await?
    }

    async fn sync_data(&self) -> io::Result<()> {
        let inner = Arc::clone(&self.inner);
        self.pool.run(move || inner.file.sync_data()).await?
    }

    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let inner = Arc::clone(&self.inner);
        self.pool.run(move || inner.read_at(offset, len)).await?
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        let inner = Arc::clone(&self.inner);
        self.pool.run(move || inner.truncate(len)).await?
    }
}
