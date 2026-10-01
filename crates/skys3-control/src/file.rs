//! The file control store, for single-node development.
//!
//! Registers are files under a root directory, at their keys' paths:
//! `<root>/cluster.json`, `<root>/nodes/<node-id>.json`, and so on, so the
//! control state can be read with ordinary tools. The store holds an
//! exclusive lock on `<root>/.lock` while it is open, so one process owns
//! the directory, and serializes its own writes; that makes `put_if`
//! linearizable without a server. Use a local file system: network file
//! systems do not reliably honor the lock.
//!
//! **Crash safety.** A write goes to a temporary file in the register's
//! directory, which is `fsync`ed, renamed over the register, and made
//! durable with an `fsync` of the directory; a directory created for a new
//! register is made durable in its parent first. A crash therefore leaves
//! each register at its old or its new value, never a mix, and a write is
//! acknowledged only once it is durable. Temporary files a crash leaves
//! behind are removed when the store is opened.
//!
//! **I/O placement.** The store uses `std::fs` on a
//! [`BlockingPool`], never the Tokio reactor (design §10.4). `skys3-io`'s
//! [`Disk`](skys3_io::Disk) does not fit: it models one flat directory of
//! append-only segment files, with no rename and no subdirectories. Reads
//! are answered from an in-memory copy of every register, loaded when the
//! store is opened and updated after each durable write.
//!
//! A delete removes the register's file and `fsync`s its directory.
//! Directories are never removed, so a later write into them needs no
//! directory creation.
//!
//! **One node.** The store serves the node it was opened for. It refuses to
//! open once another node is registered in `nodes/`, and refuses another
//! node's registration.
//!
//! **Failures.** After a failed write, the file on disk may hold either
//! value, so the store refuses every later request until it is reopened,
//! as the log takes a disk out of service after a failed sync.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use skys3_io::BlockingPool;
use skys3_types::{Generation, NodeId};
use tokio::sync::watch;

use crate::feed::ChangeFeed;
use crate::key::{KeyPrefix, RegisterKey, RegisterKind};
use crate::store::{
    ControlError, ControlStore, DeleteOutcome, Expected, PutOutcome, Version, Versioned,
};

/// The lock file in the root directory.
const LOCK_FILE: &str = ".lock";
/// The suffix of temporary files, which start with `.` like the lock file.
const TEMP_SUFFIX: &str = ".tmp";

/// Where a [`FileControlStore`] keeps its registers, and which node it
/// serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStoreConfig {
    /// The root directory, created if missing.
    pub root: PathBuf,
    /// The node the store serves.
    pub node: NodeId,
}

/// A control store in a local directory, for a single node.
///
/// Clones share the open store, which stays locked until the last clone is
/// dropped.
#[derive(Debug, Clone)]
pub struct FileControlStore {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    config: FileStoreConfig,
    pool: BlockingPool,
    /// Held by a pool thread for a whole write, so writes apply one at a
    /// time and each checks its precondition against the latest value.
    writer: Mutex<()>,
    /// Every register, as durably stored.
    registers: Mutex<BTreeMap<RegisterKey, Versioned>>,
    /// Set by a failed write.
    failed: AtomicBool,
    written: watch::Sender<()>,
    /// Holds the directory's lock while the store is open.
    _lock: File,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves its data consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FileControlStore {
    /// Opens the store in `config.root`, running its file I/O on `pool`.
    ///
    /// # Errors
    ///
    /// - [`ControlError::Unavailable`] if another process has the store
    ///   open.
    /// - [`ControlError::SecondNode`] if a node other than `config.node` is
    ///   registered.
    /// - [`ControlError::InvalidKey`] if the directory holds a file whose
    ///   path is not a register key.
    /// - [`ControlError::Io`] if the directory cannot be read.
    pub async fn open(config: FileStoreConfig, pool: BlockingPool) -> Result<Self, ControlError> {
        let job_pool = pool.clone();
        pool.run(move || Self::load(config, job_pool))
            .await
            .map_err(io::Error::from)?
    }

    fn load(config: FileStoreConfig, pool: BlockingPool) -> Result<Self, ControlError> {
        fs::create_dir_all(&config.root)?;
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.root.join(LOCK_FILE))?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(ControlError::Unavailable(format!(
                    "{} is open in another process",
                    config.root.display()
                )));
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let mut registers = BTreeMap::new();
        load_dir(&config.root, "", &mut registers)?;
        for key in registers.keys() {
            if let RegisterKind::Node(other) = key.kind()
                && other != config.node
            {
                return Err(ControlError::SecondNode {
                    serving: config.node,
                    other,
                });
            }
        }
        Ok(Self {
            shared: Arc::new(Shared {
                config,
                pool,
                writer: Mutex::new(()),
                registers: Mutex::new(registers),
                failed: AtomicBool::new(false),
                written: watch::Sender::new(()),
                _lock: lock_file,
            }),
        })
    }

    /// The store's configuration.
    #[must_use]
    pub fn config(&self) -> &FileStoreConfig {
        &self.shared.config
    }
}

impl Shared {
    fn check(&self) -> Result<(), ControlError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(ControlError::Io(io::Error::other(
                "an earlier write failed; reopen the file control store",
            )));
        }
        Ok(())
    }

    /// Runs on a pool thread.
    fn put(
        &self,
        key: &RegisterKey,
        expected: &Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let _writer = lock(&self.writer);
        self.check()?;
        if let RegisterKind::Node(other) = key.kind()
            && other != self.config.node
        {
            return Err(ControlError::SecondNode {
                serving: self.config.node.clone(),
                other,
            });
        }
        let holds = match (expected, lock(&self.registers).get(key)) {
            (Expected::Absent, None) => true,
            (Expected::Version(expected), Some(current)) => *expected == current.version,
            _ => false,
        };
        if !holds {
            return Ok(PutOutcome::PreconditionFailed);
        }
        if let Err(error) = write_durably(&self.config.root, key, &value) {
            self.failed.store(true, Ordering::Release);
            return Err(error.into());
        }
        let version = content_version(&value);
        let register = Versioned {
            value,
            version: version.clone(),
        };
        lock(&self.registers).insert(key.clone(), register);
        self.written.send_replace(());
        Ok(PutOutcome::Written(version))
    }

    /// Runs on a pool thread.
    fn delete(&self, key: &RegisterKey, expected: &Version) -> Result<DeleteOutcome, ControlError> {
        let _writer = lock(&self.writer);
        self.check()?;
        if lock(&self.registers)
            .get(key)
            .map(|current| &current.version)
            != Some(expected)
        {
            return Ok(DeleteOutcome::PreconditionFailed);
        }
        if let Err(error) = remove_durably(&self.config.root, key) {
            self.failed.store(true, Ordering::Release);
            return Err(error.into());
        }
        lock(&self.registers).remove(key);
        self.written.send_replace(());
        Ok(DeleteOutcome::Deleted)
    }
}

impl ControlStore for FileControlStore {
    type Changes = ChangeFeed<Self>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        self.shared.check()?;
        Ok(lock(&self.shared.registers).get(key).cloned())
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let shared = Arc::clone(&self.shared);
        let key = key.clone();
        // The job runs to completion even if this future is dropped, and
        // updates the in-memory copy itself, so the copy never falls
        // behind the files.
        self.shared
            .pool
            .run(move || shared.put(&key, &expected, value))
            .await
            .map_err(io::Error::from)?
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        let shared = Arc::clone(&self.shared);
        let (key, expected) = (key.clone(), expected.clone());
        // Like `put_if`, the job finishes even if this future is dropped.
        self.shared
            .pool
            .run(move || shared.delete(&key, &expected))
            .await
            .map_err(io::Error::from)?
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.shared.check()?;
        Ok(lock(&self.shared.registers)
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, register)| (key.clone(), register.version.clone()))
            .collect())
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.shared.check()?;
        Ok(ChangeFeed::notified(
            self.clone(),
            after,
            self.shared.written.subscribe(),
        ))
    }
}

/// A register's version: the first 128 bits of the SHA-256 of its value,
/// in hex. Every value carries a fresh proposal ID, so values differ.
fn content_version(value: &[u8]) -> Version {
    let digest = Sha256::digest(value);
    let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    Version::new(hex)
}

/// Loads the registers under `dir`, whose key prefix is `prefix`, and
/// removes temporary files left by a crash.
fn load_dir(
    dir: &Path,
    prefix: &str,
    registers: &mut BTreeMap<RegisterKey, Versioned>,
) -> Result<(), ControlError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            if name.ends_with(TEMP_SUFFIX) {
                fs::remove_file(entry.path())?;
            }
            continue;
        }
        let path = format!("{prefix}{name}");
        if entry.file_type()?.is_dir() {
            load_dir(&entry.path(), &format!("{path}/"), registers)?;
        } else {
            let key = RegisterKey::new(path)?;
            let value = Bytes::from(fs::read(entry.path())?);
            let version = content_version(&value);
            registers.insert(key, Versioned { value, version });
        }
    }
    Ok(())
}

/// Replaces the register file for `key` under `root` with `value`: write a
/// temporary file, `fsync` it, rename it over the register, and `fsync`
/// the directory.
fn write_durably(root: &Path, key: &RegisterKey, value: &[u8]) -> io::Result<()> {
    let (dirs, name) = match key.as_str().rsplit_once('/') {
        Some((dirs, name)) => (Some(dirs), name),
        None => (None, key.as_str()),
    };
    let mut dir = root.to_path_buf();
    for segment in dirs.into_iter().flat_map(|dirs| dirs.split('/')) {
        let child = dir.join(segment);
        match fs::create_dir(&child) {
            // Make the new directory's entry durable before anything in
            // it depends on it.
            Ok(()) => sync_dir(&dir)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        dir = child;
    }
    let temp = dir.join(format!(".{name}{TEMP_SUFFIX}"));
    let result = (|| {
        let mut file = File::create(&temp)?;
        file.write_all(value)?;
        file.sync_all()?;
        fs::rename(&temp, dir.join(name))?;
        sync_dir(&dir)
    })();
    if result.is_err() {
        // Best effort: opening the store removes it otherwise.
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Removes the register file for `key` under `root` and `fsync`s its
/// directory.
fn remove_durably(root: &Path, key: &RegisterKey) -> io::Result<()> {
    let path = root.join(key.as_str());
    fs::remove_file(&path)?;
    sync_dir(path.parent().unwrap_or(root))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn pool() -> BlockingPool {
        BlockingPool::new("control-test", NonZeroUsize::MIN).unwrap()
    }

    fn config(root: &Path, node: &str) -> FileStoreConfig {
        FileStoreConfig {
            root: root.to_path_buf(),
            node: NodeId::new(node).unwrap(),
        }
    }

    fn key(key: &str) -> RegisterKey {
        RegisterKey::new(key).unwrap()
    }

    async fn put(store: &FileControlStore, k: &str, value: &'static str) -> PutOutcome {
        store
            .put_if(
                &key(k),
                Expected::Absent,
                Bytes::from_static(value.as_bytes()),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn registers_are_files_that_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        assert_eq!(store.config().node.as_str(), "node-1");
        let written = put(&store, "shards/b-1/0.json", "{}").await;
        let PutOutcome::Written(version) = written else {
            panic!("{written:?}");
        };
        assert_eq!(version, content_version(b"{}"));
        assert_eq!(
            fs::read(dir.path().join("shards/b-1/0.json")).unwrap(),
            b"{}"
        );
        drop(store);

        // A crash left a temporary file behind.
        let temp = dir.path().join("shards/b-1/.1.json.tmp");
        fs::write(&temp, b"partial").unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        assert!(!temp.exists());
        let register = store.get(&key("shards/b-1/0.json")).await.unwrap().unwrap();
        assert_eq!(register.version, version);
        let listed = store.list(&KeyPrefix::shards()).await.unwrap();
        assert_eq!(listed, vec![(key("shards/b-1/0.json"), version)]);
    }

    #[tokio::test]
    async fn one_process_opens_the_store_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        let error = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Unavailable(_)), "{error}");
        drop(store);
        FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_second_node_can_neither_register_nor_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        assert!(matches!(
            put(&store, "nodes/node-1.json", "{}").await,
            PutOutcome::Written(_)
        ));
        let error = store
            .put_if(&key("nodes/node-2.json"), Expected::Absent, Bytes::new())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::SecondNode { .. }), "{error}");
        drop(store);

        let error = FileControlStore::open(config(dir.path(), "node-2"), pool())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "the file control store serves only node node-2; node node-1 cannot register in it"
        );
        // Written behind the store's back, as a second node sharing the
        // directory would.
        fs::write(dir.path().join("nodes/node-2.json"), b"{}").unwrap();
        let error = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::SecondNode { .. }), "{error}");
    }

    #[tokio::test]
    async fn files_outside_the_key_grammar_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("bad name"), b"{}").unwrap();
        let error = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::InvalidKey(_)), "{error}");
        let file = dir.path().join("file");
        fs::write(&file, b"").unwrap();
        let error = FileControlStore::open(config(&file, "node-1"), pool())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
    }

    #[tokio::test]
    async fn a_failed_write_takes_the_store_out_of_service() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        // A file where the write needs a directory.
        fs::write(dir.path().join("buckets"), b"").unwrap();
        let error = store
            .put_if(&key("buckets/b.json"), Expected::Absent, Bytes::new())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
        for error in [
            store.get(&key("x")).await.unwrap_err(),
            store.list(&KeyPrefix::root()).await.unwrap_err(),
            store
                .put_if(&key("x"), Expected::Absent, Bytes::new())
                .await
                .unwrap_err(),
            store.changes(Generation::ZERO).await.unwrap_err(),
        ] {
            assert!(error.to_string().contains("reopen"), "{error}");
        }
    }

    #[tokio::test]
    async fn deletes_remove_files_and_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        let PutOutcome::Written(version) = put(&store, "buckets/b.json", "{}").await else {
            panic!("the first write failed");
        };
        let stale = Version::new("stale");
        let missing = store.delete_if(&key("buckets/c.json"), &version).await;
        assert_eq!(missing.unwrap(), DeleteOutcome::PreconditionFailed);
        let outcome = store.delete_if(&key("buckets/b.json"), &stale).await;
        assert_eq!(outcome.unwrap(), DeleteOutcome::PreconditionFailed);
        let outcome = store.delete_if(&key("buckets/b.json"), &version).await;
        assert_eq!(outcome.unwrap(), DeleteOutcome::Deleted);
        assert!(!dir.path().join("buckets/b.json").exists());
        assert!(store.get(&key("buckets/b.json")).await.unwrap().is_none());
        drop(store);
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        assert!(store.list(&KeyPrefix::buckets()).await.unwrap().is_empty());
        // The emptied directory takes new registers.
        assert!(matches!(
            put(&store, "buckets/b.json", "{}").await,
            PutOutcome::Written(_)
        ));
    }

    #[tokio::test]
    async fn a_failed_delete_takes_the_store_out_of_service() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool())
            .await
            .unwrap();
        let PutOutcome::Written(version) = put(&store, "buckets/b.json", "{}").await else {
            panic!("the first write failed");
        };
        // Removed behind the store's back.
        fs::remove_file(dir.path().join("buckets/b.json")).unwrap();
        let error = store
            .delete_if(&key("buckets/b.json"), &version)
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
        let error = store.get(&key("buckets/b.json")).await.unwrap_err();
        assert!(error.to_string().contains("reopen"), "{error}");
    }

    #[tokio::test]
    async fn a_shut_down_pool_fails_requests() {
        let dir = tempfile::tempdir().unwrap();
        let pool = pool();
        let store = FileControlStore::open(config(dir.path(), "node-1"), pool.clone())
            .await
            .unwrap();
        pool.shutdown();
        let error = store
            .put_if(&key("x"), Expected::Absent, Bytes::new())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
        let error = store
            .delete_if(&key("x"), &Version::new("1"))
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
        let error = FileControlStore::open(config(dir.path(), "node-1"), pool)
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Io(_)), "{error}");
    }
}
