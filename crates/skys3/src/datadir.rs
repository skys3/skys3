//! The node's data directory and its disks: the node's identity, which
//! disks it was created with, and disks fenced after an I/O failure
//! (design §10.1, §10.4).
//!
//! The data directory holds `node.json`, written once when the node is
//! created: its cluster, its ID, a random instance ID that names this data
//! directory (the file control store records it as its owner), and its
//! disks, each with a label and the path it was configured at. Each disk directory holds `disk.json`, which
//! names the cluster, the node, and the disk's label, so a disk mounted at
//! the wrong path, or another node's disk, is never used. A node keeps the
//! disks it was created with: shard replicas are placed on disks by label
//! ([`ShardSet::disk_of`](skys3_shard::ShardSet::disk_of)).
//!
//! A disk that a write or sync failed on is fenced with
//! `out-of-service.json`, which records the host's boot ID. After a failed
//! sync the page cache can show bytes the disk lost, so the disk is used
//! again only after the host restarts (§10.4): a node refuses to start
//! while the boot ID is the same.
//!
//! Everything here is blocking file I/O; the node runs it on a blocking
//! pool.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use rand::Rng;
use serde::{Deserialize, Serialize};
use skys3_config::NodeConfig;
use skys3_types::{ClusterId, Label, NodeId};

/// The node's identity file in the data directory.
pub const NODE_FILE: &str = "node.json";
/// A disk's identity file in its directory.
pub const DISK_FILE: &str = "disk.json";
/// The fence a failed disk gets.
pub const FENCE_FILE: &str = "out-of-service.json";
/// The lock file that keeps a second process out of the data directory.
const LOCK_FILE: &str = ".lock";
/// Where Linux reports the current boot's ID.
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";
/// The format of `node.json`, `disk.json`, the fence, and the file control
/// store's owner file.
pub(crate) const FORMAT: u32 = 1;

/// Why the data directory cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum DataDirError {
    /// A file could not be read or written.
    #[error("{path}: {source}")]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The error.
        #[source]
        source: io::Error,
    },
    /// Another process has the data directory open.
    #[error("{0} is in use by another process")]
    Locked(PathBuf),
    /// A file holds something this build does not accept.
    #[error("{path} is invalid: {reason}")]
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong.
        reason: String,
    },
    /// The data directory, or a disk, belongs to another cluster or node,
    /// or the configured disks are not the ones the node was created with.
    #[error("{0}")]
    Mismatch(String),
    /// A disk was taken out of service in this boot.
    #[error(
        "disk {label} ({path}) was taken out of service after an I/O error ({error}); it is \
         used again only after the host restarts, or once an operator has checked it and \
         removed {FENCE_FILE}"
    )]
    Fenced {
        /// The disk's label.
        label: Label,
        /// The disk's directory.
        path: PathBuf,
        /// The error that took it out of service.
        error: String,
    },
}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> DataDirError + '_ {
    move |source| DataDirError::Io {
        path: path.to_owned(),
        source,
    }
}

/// One disk as `node.json` records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedDisk {
    label: Label,
    path: PathBuf,
}

/// `node.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeFile {
    format: u32,
    cluster_id: ClusterId,
    node_id: NodeId,
    /// Random, set when the node is created: two data directories never
    /// share it, even when configured with the same node ID.
    instance_id: String,
    disks: Vec<RecordedDisk>,
}

/// `disk.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskFile {
    format: u32,
    cluster_id: ClusterId,
    node_id: NodeId,
    label: Label,
}

/// `out-of-service.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceFile {
    format: u32,
    /// The boot the disk failed in, or `None` where the host has no boot
    /// ID; such a fence lasts until an operator removes it.
    boot_id: Option<String>,
    error: String,
}

/// A disk of the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskDir {
    /// The disk's label, which places shard replicas.
    pub label: Label,
    /// The directory of its log segments.
    pub path: PathBuf,
}

/// The node's open data directory. The directory stays locked against
/// other processes while this value lives.
#[derive(Debug)]
pub struct DataDir {
    path: PathBuf,
    node_id: NodeId,
    instance_id: String,
    disks: Vec<DiskDir>,
    _lock: File,
}

impl DataDir {
    /// Opens the data directory of `config` for `cluster`, creating the
    /// node when the directory holds none. `boot_id` is the host's current
    /// boot ([`boot_id`]).
    ///
    /// # Errors
    ///
    /// [`DataDirError`] if the directory is in use, belongs to another
    /// cluster or node, the disks are not the node's, or a disk is fenced.
    pub fn open(
        config: &NodeConfig,
        cluster: &ClusterId,
        boot_id: Option<&str>,
    ) -> Result<Self, DataDirError> {
        let path = config.data_dir.clone();
        fs::create_dir_all(&path).map_err(io_error(&path))?;
        let lock = lock(&path)?;
        let node = match read_json::<NodeFile>(&path.join(NODE_FILE))? {
            Some(node) => check_node(node, config, cluster)?,
            None => create_node(config, cluster, &path)?,
        };
        let mut disks = Vec::with_capacity(node.disks.len());
        for recorded in &node.disks {
            check_disk(&node, recorded)?;
            unfence(recorded, boot_id)?;
            disks.push(DiskDir {
                label: recorded.label.clone(),
                path: recorded.path.clone(),
            });
        }
        Ok(Self {
            path,
            node_id: node.node_id,
            instance_id: node.instance_id,
            disks,
            _lock: lock,
        })
    }

    /// The data directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The node's ID.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// The data directory's instance ID: random, set when the node was
    /// created.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// The node's disks, in the order they were configured when the node
    /// was created.
    #[must_use]
    pub fn disks(&self) -> &[DiskDir] {
        &self.disks
    }
}

/// Locks the data directory against other processes.
fn lock(dir: &Path) -> Result<File, DataDirError> {
    let path = dir.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(io_error(&path))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(DataDirError::Locked(dir.to_owned())),
        Err(TryLockError::Error(error)) => Err(io_error(&path)(error)),
    }
}

/// Checks an existing node against the configuration.
fn check_node(
    node: NodeFile,
    config: &NodeConfig,
    cluster: &ClusterId,
) -> Result<NodeFile, DataDirError> {
    if node.cluster_id != *cluster {
        return Err(DataDirError::Mismatch(format!(
            "{} belongs to cluster {}, not {cluster}",
            config.data_dir.display(),
            node.cluster_id
        )));
    }
    if let Some(id) = &config.node_id
        && *id != node.node_id
    {
        return Err(DataDirError::Mismatch(format!(
            "{} belongs to node {}, but [node] node_id is {id}",
            config.data_dir.display(),
            node.node_id
        )));
    }
    let recorded: BTreeSet<_> = node.disks.iter().map(|disk| &disk.path).collect();
    let configured: BTreeSet<_> = config.disks.iter().collect();
    if recorded != configured {
        return Err(DataDirError::Mismatch(format!(
            "[node] disks lists {configured:?}, but the node was created with {recorded:?}; a \
             node keeps its disks, because each shard replica's records stay on one disk"
        )));
    }
    Ok(node)
}

/// Creates the node: `node.json` first, which records every disk, then
/// each disk's `disk.json`. A disk that `node.json` records but that has
/// no `disk.json` yet, after a crash in between, gets it at the next start.
fn create_node(
    config: &NodeConfig,
    cluster: &ClusterId,
    dir: &Path,
) -> Result<NodeFile, DataDirError> {
    let node_id = config.node_id.clone().unwrap_or_else(generate_node_id);
    let mut disks = Vec::with_capacity(config.disks.len());
    for (index, path) in config.disks.iter().enumerate() {
        fs::create_dir_all(path).map_err(io_error(path))?;
        if path.join(DISK_FILE).exists() || has_segments(path)? {
            return Err(DataDirError::Mismatch(format!(
                "{} holds another node's data; a new node needs empty disks",
                path.display()
            )));
        }
        disks.push(RecordedDisk {
            label: Label::new(format!("disk-{index}")).expect("the label is valid"),
            path: path.clone(),
        });
    }
    let node = NodeFile {
        format: FORMAT,
        cluster_id: cluster.clone(),
        node_id,
        instance_id: random_base32(26),
        disks,
    };
    write_json(dir, NODE_FILE, &node)?;
    tracing::info!(node_id = %node.node_id, data_dir = %dir.display(), "created the node");
    Ok(node)
}

/// A node ID for a node whose configuration sets none: `n-` and 16 random
/// base-32 characters.
fn generate_node_id() -> NodeId {
    NodeId::new(format!("n-{}", random_base32(16))).expect("the alphabet makes a valid node ID")
}

/// `len` random lowercase base-32 characters.
fn random_base32(len: usize) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| char::from(ALPHABET[rng.random_range(0..ALPHABET.len())]))
        .collect()
}

/// Whether `dir` holds log segments.
fn has_segments(dir: &Path) -> Result<bool, DataDirError> {
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        if skys3_log::segment::parse_file_name(&entry.file_name().to_string_lossy()).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Checks that the disk at a recorded path is the node's disk, and writes
/// its `disk.json` if node creation stopped before it.
fn check_disk(node: &NodeFile, recorded: &RecordedDisk) -> Result<(), DataDirError> {
    let path = &recorded.path;
    let expected = DiskFile {
        format: FORMAT,
        cluster_id: node.cluster_id.clone(),
        node_id: node.node_id.clone(),
        label: recorded.label.clone(),
    };
    match read_json::<DiskFile>(&path.join(DISK_FILE))? {
        Some(found) if found == expected => Ok(()),
        Some(found) => Err(DataDirError::Mismatch(format!(
            "{} is disk {} of node {} in cluster {}, not disk {} of node {}",
            path.display(),
            found.label,
            found.node_id,
            found.cluster_id,
            recorded.label,
            node.node_id
        ))),
        None if path.is_dir() && !has_segments(path)? => write_json(path, DISK_FILE, &expected),
        None => Err(DataDirError::Mismatch(format!(
            "{} has no {DISK_FILE}: it is not disk {} of this node, or is not mounted",
            path.display(),
            recorded.label
        ))),
    }
}

/// Lifts a disk's fence if the host restarted since it was set.
fn unfence(disk: &RecordedDisk, boot_id: Option<&str>) -> Result<(), DataDirError> {
    let path = disk.path.join(FENCE_FILE);
    let Some(fence) = read_json::<FenceFile>(&path)? else {
        return Ok(());
    };
    match (fence.boot_id.as_deref(), boot_id) {
        (Some(failed), Some(now)) if failed != now => {
            tracing::warn!(
                disk = %disk.label,
                error = %fence.error,
                "the host restarted since this disk was taken out of service; using it again"
            );
            fs::remove_file(&path).map_err(io_error(&path))?;
            sync_dir(&disk.path)
        }
        _ => Err(DataDirError::Fenced {
            label: disk.label.clone(),
            path: disk.path.clone(),
            error: fence.error,
        }),
    }
}

/// Fences `disk` after `error` took it out of service in boot `boot_id`.
///
/// # Errors
///
/// If the fence cannot be written, most likely because the disk itself
/// fails; the node then keeps the disk out of service only until it exits.
pub fn fence(disk: &DiskDir, boot_id: Option<&str>, error: &str) -> Result<(), DataDirError> {
    let fence = FenceFile {
        format: FORMAT,
        boot_id: boot_id.map(str::to_owned),
        error: error.to_owned(),
    };
    write_json(&disk.path, FENCE_FILE, &fence)
}

/// The host's current boot ID, or `None` where the host reports none.
#[must_use]
pub fn boot_id() -> Option<String> {
    let id = fs::read_to_string(BOOT_ID_PATH).ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_owned())
}

/// Reads a JSON file, or `None` if it does not exist.
pub(crate) fn read_json<T: for<'de> Deserialize<'de> + HasFormat>(
    path: &Path,
) -> Result<Option<T>, DataDirError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path)(error)),
    };
    let value: T = serde_json::from_slice(&bytes).map_err(|error| DataDirError::Invalid {
        path: path.to_owned(),
        reason: error.to_string(),
    })?;
    if value.format() != FORMAT {
        return Err(DataDirError::Invalid {
            path: path.to_owned(),
            reason: format!(
                "format {} is not {FORMAT}, the one this build reads",
                value.format()
            ),
        });
    }
    Ok(Some(value))
}

/// Writes `value` as `dir/name` durably: a synced temporary file, renamed
/// over the name, then a sync of the directory.
pub(crate) fn write_json(
    dir: &Path,
    name: &str,
    value: &impl Serialize,
) -> Result<(), DataDirError> {
    let bytes = serde_json::to_vec_pretty(value).expect("the file serializes");
    let temporary = dir.join(format!(".{name}.tmp"));
    let mut file = File::create(&temporary).map_err(io_error(&temporary))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_error(&temporary))?;
    let path = dir.join(name);
    fs::rename(&temporary, &path).map_err(io_error(&path))?;
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> Result<(), DataDirError> {
    File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(io_error(dir))
}

/// The files with a format number.
pub(crate) trait HasFormat {
    fn format(&self) -> u32;
}

impl HasFormat for NodeFile {
    fn format(&self) -> u32 {
        self.format
    }
}

impl HasFormat for DiskFile {
    fn format(&self) -> u32 {
        self.format
    }
}

impl HasFormat for FenceFile {
    fn format(&self) -> u32 {
        self.format
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(dir: &Path, disks: &[&str]) -> NodeConfig {
        NodeConfig {
            node_id: None,
            data_dir: dir.join("data"),
            disks: disks.iter().map(|disk| dir.join(disk)).collect(),
        }
    }

    fn cluster(id: &str) -> ClusterId {
        ClusterId::new(id).unwrap()
    }

    fn mismatch(result: Result<DataDir, DataDirError>) -> String {
        match result {
            Err(DataDirError::Mismatch(message)) => message,
            other => panic!("expected a mismatch, got {other:?}"),
        }
    }

    #[test]
    fn a_node_keeps_its_id_and_disks() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path(), &["d0", "d1"]);
        let first = DataDir::open(&config, &cluster("c"), Some("boot-1")).unwrap();
        let id = first.node_id().clone();
        assert!(id.as_str().starts_with("n-"));
        assert_eq!(first.path(), dir.path().join("data"));
        let labels: Vec<_> = first.disks().iter().map(|d| d.label.as_str()).collect();
        assert_eq!(labels, ["disk-0", "disk-1"]);
        assert!(matches!(
            DataDir::open(&config, &cluster("c"), None),
            Err(DataDirError::Locked(_))
        ));
        drop(first);

        // The disks may be listed in another order; their labels stay.
        let mut reordered = config.clone();
        reordered.disks.reverse();
        let again = DataDir::open(&reordered, &cluster("c"), None).unwrap();
        assert_eq!(again.node_id(), &id);
        assert_eq!(again.disks()[0].label.as_str(), "disk-0");
        assert_eq!(again.disks()[0].path, dir.path().join("d0"));
        drop(again);

        let message = mismatch(DataDir::open(&config, &cluster("other"), None));
        assert!(message.contains("belongs to cluster c"), "{message}");
        let mut named = config.clone();
        named.node_id = Some(NodeId::new("node-9").unwrap());
        let message = mismatch(DataDir::open(&named, &cluster("c"), None));
        assert!(message.contains("node_id is node-9"), "{message}");
        let mut fewer = config.clone();
        fewer.disks.pop();
        let message = mismatch(DataDir::open(&fewer, &cluster("c"), None));
        assert!(message.contains("keeps its disks"), "{message}");
    }

    #[test]
    fn disks_must_be_the_nodes_own() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config(dir.path(), &["d0"]);
        config.node_id = Some(NodeId::new("node-1").unwrap());
        drop(DataDir::open(&config, &cluster("c"), None).unwrap());

        // Another node cannot be created over the disk.
        let mut other = config.clone();
        other.data_dir = dir.path().join("other");
        let message = mismatch(DataDir::open(&other, &cluster("c"), None));
        assert!(message.contains("another node's data"), "{message}");

        // A disk without its identity file, holding segments, is refused.
        let disk = dir.path().join("d0");
        fs::remove_file(disk.join(DISK_FILE)).unwrap();
        fs::write(disk.join("hot-0000000000000001.seg"), b"").unwrap();
        let message = mismatch(DataDir::open(&config, &cluster("c"), None));
        assert!(message.contains("has no disk.json"), "{message}");
        // Without segments, node creation stopped early: it is finished.
        fs::remove_file(disk.join("hot-0000000000000001.seg")).unwrap();
        drop(DataDir::open(&config, &cluster("c"), None).unwrap());
        assert!(disk.join(DISK_FILE).exists());

        // A disk of another node is refused.
        let foreign = DiskFile {
            format: FORMAT,
            cluster_id: cluster("c"),
            node_id: NodeId::new("node-2").unwrap(),
            label: Label::new("disk-0").unwrap(),
        };
        write_json(&disk, DISK_FILE, &foreign).unwrap();
        let message = mismatch(DataDir::open(&config, &cluster("c"), None));
        assert!(message.contains("of node node-2"), "{message}");

        fs::write(disk.join(DISK_FILE), b"{\"format\": 2}").unwrap();
        assert!(matches!(
            DataDir::open(&config, &cluster("c"), None),
            Err(DataDirError::Invalid { .. })
        ));
        fs::write(
            disk.join(DISK_FILE),
            serde_json::to_vec(&DiskFile {
                format: 2,
                ..foreign
            })
            .unwrap(),
        )
        .unwrap();
        let error = DataDir::open(&config, &cluster("c"), None).unwrap_err();
        assert!(error.to_string().contains("format 2"), "{error}");
    }

    #[test]
    fn a_fenced_disk_waits_for_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path(), &["d0"]);
        let data = DataDir::open(&config, &cluster("c"), Some("boot-1")).unwrap();
        let disk = data.disks()[0].clone();
        fence(&disk, Some("boot-1"), "sync failed").unwrap();
        drop(data);

        let error = DataDir::open(&config, &cluster("c"), Some("boot-1")).unwrap_err();
        assert!(matches!(error, DataDirError::Fenced { .. }));
        assert!(error.to_string().contains("sync failed"), "{error}");
        // Without a boot ID the fence stays.
        assert!(DataDir::open(&config, &cluster("c"), None).is_err());
        // After a restart the disk is used again.
        drop(DataDir::open(&config, &cluster("c"), Some("boot-2")).unwrap());
        assert!(!disk.path.join(FENCE_FILE).exists());
    }

    #[test]
    fn io_errors_name_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        fs::write(&file, b"").unwrap();
        let config = NodeConfig {
            node_id: None,
            data_dir: file.join("data"),
            disks: Vec::new(),
        };
        let error = DataDir::open(&config, &cluster("c"), None).unwrap_err();
        assert!(matches!(error, DataDirError::Io { .. }));
        assert!(error.to_string().contains("file"), "{error}");
        assert!(boot_id().is_none_or(|id| !id.is_empty()));
    }
}
