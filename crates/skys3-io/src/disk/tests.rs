//! Tests of the disk contract, run against both the real and the simulated
//! disk, and of the simulated disk's crash and fault behavior.

use std::io::ErrorKind;
use std::num::NonZeroUsize;

use bytes::Bytes;

use super::*;
use crate::pool::BlockingPool;

// ---------------------------------------------------------------------------
// Contract tests, shared by both implementations.
// ---------------------------------------------------------------------------

async fn append_and_read_back<D: Disk>(disk: D) {
    let file = disk.create("seg-1").await.unwrap();
    assert!(file.is_empty());
    assert_eq!(file.append(Bytes::from_static(b"hello ")).await.unwrap(), 0);
    assert_eq!(file.append(Bytes::from_static(b"world")).await.unwrap(), 6);
    assert_eq!(file.len(), 11);
    assert!(!file.is_empty());
    assert_eq!(&file.read_at(0, 11).await.unwrap()[..], b"hello world");
    assert_eq!(&file.read_at(6, 5).await.unwrap()[..], b"world");
    assert_eq!(&file.read_at(11, 0).await.unwrap()[..], b"");
    file.sync_data().await.unwrap();
    disk.sync_dir().await.unwrap();

    let error = file.read_at(6, 6).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    let error = file.read_at(12, 0).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    let error = file.read_at(u64::MAX, 1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);

    // A second handle sees the same file and appends at its end.
    let other = disk.open("seg-1").await.unwrap();
    assert_eq!(other.len(), 11);
    assert_eq!(other.append(Bytes::from_static(b"!")).await.unwrap(), 11);
    assert_eq!(&file.read_at(0, 12).await.unwrap()[..], b"hello world!");
}

async fn names_and_errors<D: Disk>(disk: D) {
    assert_eq!(disk.list().await.unwrap(), Vec::<String>::new());
    drop(disk.create("b").await.unwrap());
    drop(disk.create("a").await.unwrap());
    assert_eq!(disk.list().await.unwrap(), ["a", "b"]);

    assert_eq!(
        disk.create("a").await.unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        disk.open("c").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        disk.remove("c").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    let long = "x".repeat(MAX_NAME_LEN + 1);
    for name in ["", ".", "..", "a/b", "a\0b", long.as_str()] {
        assert_eq!(
            disk.create(name).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            disk.open(name).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            disk.remove(name).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    drop(disk.create(&"y".repeat(MAX_NAME_LEN)).await.unwrap());

    disk.remove("a").await.unwrap();
    disk.sync_dir().await.unwrap();
    assert_eq!(
        disk.list().await.unwrap(),
        ["b".to_owned(), "y".repeat(MAX_NAME_LEN)]
    );
}

async fn removed_file_stays_readable_through_open_handles<D: Disk>(disk: D) {
    let file = disk.create("seg").await.unwrap();
    file.append(Bytes::from_static(b"payload")).await.unwrap();
    disk.remove("seg").await.unwrap();
    assert_eq!(
        disk.open("seg").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(&file.read_at(0, 7).await.unwrap()[..], b"payload");
    // The name is free again.
    let new = disk.create("seg").await.unwrap();
    assert!(new.is_empty());
    // Opening the name finds the new file, shared with its creator, while
    // the old handle keeps the removed one.
    new.append(Bytes::from_static(b"fresh")).await.unwrap();
    let opened = disk.open("seg").await.unwrap();
    assert_eq!(opened.len(), 5);
    assert_eq!(opened.append(Bytes::from_static(b"!")).await.unwrap(), 5);
    assert_eq!(new.len(), 6);
    assert_eq!(&file.read_at(0, 7).await.unwrap()[..], b"payload");
    assert_eq!(file.len(), 7);
}

async fn truncate_cuts_the_tail<D: Disk>(disk: D) {
    let file = disk.create("seg").await.unwrap();
    file.append(Bytes::from_static(b"good-record|torn-rec"))
        .await
        .unwrap();
    file.truncate(12).await.unwrap();
    assert_eq!(file.len(), 12);
    assert_eq!(file.append(Bytes::from_static(b"next")).await.unwrap(), 12);
    assert_eq!(&file.read_at(0, 16).await.unwrap()[..], b"good-record|next");
    let error = file.truncate(17).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    file.truncate(0).await.unwrap();
    assert!(file.is_empty());
}

macro_rules! contract_tests {
    ($module:ident, $disk:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn append_and_read_back() {
                let (disk, _guard) = $disk;
                super::append_and_read_back(disk).await;
            }

            #[tokio::test]
            async fn names_and_errors() {
                let (disk, _guard) = $disk;
                super::names_and_errors(disk).await;
            }

            #[tokio::test]
            async fn removed_file_stays_readable_through_open_handles() {
                let (disk, _guard) = $disk;
                super::removed_file_stays_readable_through_open_handles(disk).await;
            }

            #[tokio::test]
            async fn truncate_cuts_the_tail() {
                let (disk, _guard) = $disk;
                super::truncate_cuts_the_tail(disk).await;
            }
        }
    };
}

fn test_pool() -> BlockingPool {
    BlockingPool::new("disk-test", NonZeroUsize::new(2).unwrap()).unwrap()
}

async fn real_disk() -> (RealDisk, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let disk = RealDisk::open(dir.path().join("segments"), test_pool())
        .await
        .unwrap();
    (disk, dir)
}

contract_tests!(real, real_disk().await);
contract_tests!(sim, (SimDisk::new(1).mount(), ()));

// ---------------------------------------------------------------------------
// Real disk.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn real_disk_files_live_in_its_directory() {
    let (disk, _dir) = real_disk().await;
    assert!(disk.path().ends_with("segments"));
    assert_eq!(disk.pool().name(), "disk-test");
    let file = disk.create("seg").await.unwrap();
    file.append(Bytes::from_static(b"abc")).await.unwrap();
    file.sync_data().await.unwrap();
    assert_eq!(std::fs::read(disk.path().join("seg")).unwrap(), b"abc");

    // Subdirectories are not segment files.
    std::fs::create_dir(disk.path().join("subdir")).unwrap();
    assert_eq!(disk.list().await.unwrap(), ["seg"]);

    // Reopening the directory finds the file with its length.
    let again = RealDisk::open(disk.path(), disk.pool().clone())
        .await
        .unwrap();
    assert_eq!(again.open("seg").await.unwrap().len(), 3);
}

#[tokio::test]
async fn real_disk_rejects_a_path_that_is_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-dir");
    std::fs::write(&path, b"").unwrap();
    assert!(RealDisk::open(path, test_pool()).await.is_err());
}

/// Races `remove` against `create` and `open` of the same name. Every handle
/// to the file that holds the name afterwards must share one length, or two
/// writers would append at the same offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_disk_namespace_races_keep_one_open_file_per_name() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BlockingPool::new("race", NonZeroUsize::new(4).unwrap()).unwrap();
    let disk = RealDisk::open(dir.path(), pool).await.unwrap();
    for _ in 0..500 {
        let old = disk.create("seg").await.unwrap();
        let remove = tokio::spawn({
            let disk = disk.clone();
            async move { disk.remove("seg").await }
        });
        let create = tokio::spawn({
            let disk = disk.clone();
            async move { disk.create("seg").await }
        });
        let open = tokio::spawn({
            let disk = disk.clone();
            async move { disk.open("seg").await }
        });
        remove.await.unwrap().unwrap();
        let created = create.await.unwrap();
        let opened = open.await.unwrap();
        drop(old);

        match created {
            Ok(created) => {
                created.append(Bytes::from_static(b"x")).await.unwrap();
                let reopened = disk.open("seg").await.unwrap();
                assert_eq!(reopened.len(), created.len());
                assert_eq!(reopened.append(Bytes::from_static(b"y")).await.unwrap(), 1);
                assert_eq!(created.len(), 2);
                if let Ok(opened) = opened {
                    // Opened either the removed file or the new one.
                    assert!(opened.len() == 0 || opened.len() == 2, "{}", opened.len());
                }
                disk.remove("seg").await.unwrap();
            }
            Err(error) => {
                // The create ran before the remove.
                assert_eq!(error.kind(), ErrorKind::AlreadyExists);
                assert!(disk.open("seg").await.is_err());
            }
        }
    }
}

#[tokio::test]
async fn real_disk_does_not_reuse_an_open_file_for_a_replaced_name() {
    let (disk, _dir) = real_disk().await;
    let old = disk.create("seg").await.unwrap();
    old.append(Bytes::from_static(b"old")).await.unwrap();
    // Another process replaces the file behind the disk's back.
    std::fs::remove_file(disk.path().join("seg")).unwrap();
    std::fs::write(disk.path().join("seg"), b"replaced").unwrap();

    let opened = disk.open("seg").await.unwrap();
    assert_eq!(&opened.read_at(0, 8).await.unwrap()[..], b"replaced");
    assert_eq!(old.len(), 3);
    // Later opens share the file now registered for the name.
    assert_eq!(opened.append(Bytes::from_static(b"!")).await.unwrap(), 8);
    assert_eq!(disk.open("seg").await.unwrap().len(), 9);
}

#[tokio::test]
async fn real_disk_io_runs_on_its_pool() {
    let (disk, _dir) = real_disk().await;
    let file = disk.create("seg").await.unwrap();
    disk.pool().shutdown();
    for error in [
        file.append(Bytes::from_static(b"x")).await.unwrap_err(),
        file.sync_data().await.unwrap_err(),
        file.read_at(0, 0).await.unwrap_err(),
        file.truncate(0).await.unwrap_err(),
        disk.create("other").await.unwrap_err(),
        disk.sync_dir().await.unwrap_err(),
        disk.list().await.unwrap_err(),
        disk.remove("seg").await.unwrap_err(),
    ] {
        assert!(
            error.get_ref().unwrap().is::<crate::PoolClosed>(),
            "{error}"
        );
    }
}

// ---------------------------------------------------------------------------
// Simulated disk: crashes.
// ---------------------------------------------------------------------------

async fn read_all(file: &SimFile) -> Vec<u8> {
    file.read_at(0, file.len() as usize).await.unwrap().to_vec()
}

#[tokio::test]
async fn crash_loses_unsynced_data_and_keeps_synced_data() {
    let disk = SimDisk::new(7);
    let mount = disk.mount();
    let file = mount.create("seg").await.unwrap();
    mount.sync_dir().await.unwrap();
    file.append(Bytes::from_static(b"synced;")).await.unwrap();
    file.sync_data().await.unwrap();
    file.append(Bytes::from_static(b"unsynced")).await.unwrap();
    assert_eq!(
        disk.file_info("seg"),
        Some(SimFileInfo {
            written: 15,
            synced: 7,
            durable_entry: true
        })
    );

    disk.crash();
    assert_eq!(disk.crashes(), 1);

    let mount = disk.mount();
    let file = mount.open("seg").await.unwrap();
    assert_eq!(read_all(&file).await, b"synced;");
    assert_eq!(
        disk.file_info("seg"),
        Some(SimFileInfo {
            written: 7,
            synced: 7,
            durable_entry: true
        })
    );
    assert_eq!(disk.used_bytes(), 7);
}

#[tokio::test]
async fn crash_loses_files_whose_directory_entry_was_not_synced() {
    let disk = SimDisk::new(7);
    let mount = disk.mount();
    let file = mount.create("new-seg").await.unwrap();
    file.append(Bytes::from_static(b"data")).await.unwrap();
    // The data is synced, but the directory entry is not.
    file.sync_data().await.unwrap();
    assert_eq!(
        disk.file_info("new-seg").map(|i| i.durable_entry),
        Some(false)
    );

    disk.crash();
    let mount = disk.mount();
    assert_eq!(mount.list().await.unwrap(), Vec::<String>::new());
    assert_eq!(
        mount.open("new-seg").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(disk.used_bytes(), 0);
}

#[tokio::test]
async fn crash_restores_files_whose_removal_was_not_synced() {
    let disk = SimDisk::new(7);
    let mount = disk.mount();
    let file = mount.create("seg").await.unwrap();
    file.append(Bytes::from_static(b"old")).await.unwrap();
    file.sync_data().await.unwrap();
    mount.sync_dir().await.unwrap();

    mount.remove("seg").await.unwrap();
    let replacement = mount.create("seg").await.unwrap();
    replacement
        .append(Bytes::from_static(b"new"))
        .await
        .unwrap();
    replacement.sync_data().await.unwrap();
    disk.crash();

    let mount = disk.mount();
    let file = mount.open("seg").await.unwrap();
    assert_eq!(read_all(&file).await, b"old");

    // A synced removal is durable.
    mount.remove("seg").await.unwrap();
    mount.sync_dir().await.unwrap();
    drop(file);
    assert_eq!(disk.used_bytes(), 0);
    disk.crash();
    assert_eq!(disk.mount().list().await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn crash_undoes_unsynced_truncation() {
    let disk = SimDisk::new(7);
    let mount = disk.mount();
    let file = mount.create("seg").await.unwrap();
    mount.sync_dir().await.unwrap();
    file.append(Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    file.sync_data().await.unwrap();
    file.truncate(4).await.unwrap();
    file.append(Bytes::from_static(b"ab")).await.unwrap();
    assert_eq!(disk.used_bytes(), 10);
    disk.crash();

    let mount = disk.mount();
    let file = mount.open("seg").await.unwrap();
    assert_eq!(read_all(&file).await, b"0123456789");

    // Once synced, the truncation and the new tail are durable.
    file.truncate(4).await.unwrap();
    file.append(Bytes::from_static(b"ab")).await.unwrap();
    file.sync_data().await.unwrap();
    assert_eq!(disk.used_bytes(), 6);
    disk.crash();
    let file = disk.mount().open("seg").await.unwrap();
    assert_eq!(read_all(&file).await, b"0123ab");
}

#[tokio::test]
async fn handles_from_before_a_crash_are_stale() {
    let disk = SimDisk::new(7);
    let old_mount = disk.mount();
    let old_file = old_mount.create("seg").await.unwrap();
    old_mount.sync_dir().await.unwrap();
    old_file.append(Bytes::from_static(b"abc")).await.unwrap();
    old_file.sync_data().await.unwrap();
    disk.crash();

    assert_eq!(old_file.len(), 0);
    assert!(old_file.append(Bytes::from_static(b"x")).await.is_err());
    assert!(old_file.sync_data().await.is_err());
    assert!(old_file.read_at(0, 1).await.is_err());
    assert!(old_file.truncate(0).await.is_err());
    assert!(old_mount.create("other").await.is_err());
    assert!(old_mount.open("seg").await.is_err());
    assert!(old_mount.remove("seg").await.is_err());
    assert!(old_mount.list().await.is_err());
    assert!(old_mount.sync_dir().await.is_err());

    // Dropping a stale handle does not disturb the new incarnation.
    let file = disk.mount().open("seg").await.unwrap();
    drop(old_file);
    assert_eq!(read_all(&file).await, b"abc");
    assert_eq!(old_mount.disk().crashes(), 1);
}

// ---------------------------------------------------------------------------
// Simulated disk: faults.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failed_sync_loses_data_even_if_a_later_sync_succeeds() {
    let disk = SimDisk::new(7);
    let mount = disk.mount();
    let file = mount.create("seg").await.unwrap();
    mount.sync_dir().await.unwrap();
    file.append(Bytes::from_static(b"kept|")).await.unwrap();
    file.sync_data().await.unwrap();
    file.append(Bytes::from_static(b"lost|")).await.unwrap();

    disk.fail_next_syncs(1);
    let error = file.sync_data().await.unwrap_err();
    assert_eq!(error.to_string(), "injected sync error");
    // Still readable before the crash, and the retry reports success.
    assert_eq!(read_all(&file).await, b"kept|lost|");
    file.append(Bytes::from_static(b"later")).await.unwrap();
    file.sync_data().await.unwrap();
    disk.crash();

    let file = disk.mount().open("seg").await.unwrap();
    assert_eq!(read_all(&file).await, b"kept|\0\0\0\0\0later");
}

#[tokio::test]
async fn failed_directory_sync_leaves_entries_unsynced() {
    let disk = SimDisk::with_faults(
        7,
        SimDiskFaults {
            sync_error_probability: 1.0,
            ..SimDiskFaults::default()
        },
    );
    let mount = disk.mount();
    drop(mount.create("seg").await.unwrap());
    assert!(mount.sync_dir().await.is_err());
    assert_eq!(disk.file_info("seg").map(|i| i.durable_entry), Some(false));
    disk.crash();
    assert_eq!(disk.mount().list().await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn torn_writes_keep_a_prefix_of_the_unsynced_bytes() {
    let payload: Vec<u8> = (0..=255).collect();
    let mut lengths = std::collections::BTreeSet::new();
    for seed in 0..32 {
        let disk = SimDisk::with_faults(
            seed,
            SimDiskFaults {
                torn_write_probability: 1.0,
                ..SimDiskFaults::default()
            },
        );
        let mount = disk.mount();
        let file = mount.create("seg").await.unwrap();
        mount.sync_dir().await.unwrap();
        file.append(Bytes::from_static(b"synced")).await.unwrap();
        file.sync_data().await.unwrap();
        file.append(Bytes::from(payload.clone())).await.unwrap();
        disk.crash();

        let contents = read_all(&disk.mount().open("seg").await.unwrap()).await;
        let (synced, torn) = contents.split_at(6);
        assert_eq!(synced, b"synced");
        assert_eq!(torn, &payload[..torn.len()]);
        lengths.insert(torn.len());
    }
    assert!(lengths.len() > 10, "torn lengths do not vary: {lengths:?}");
}

#[tokio::test]
async fn full_disk_writes_what_fits_and_fails() {
    let disk = SimDisk::with_faults(
        7,
        SimDiskFaults {
            capacity: Some(10),
            ..SimDiskFaults::default()
        },
    );
    assert_eq!(disk.faults().capacity, Some(10));
    let mount = disk.mount();
    let file = mount.create("seg").await.unwrap();
    file.append(Bytes::from_static(b"123456")).await.unwrap();
    let error = file
        .append(Bytes::from_static(b"abcdef"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::StorageFull);
    assert_eq!(read_all(&file).await, b"123456abcd");
    assert_eq!(disk.used_bytes(), 10);
    let error = mount.create("other").await.map(|f| f.len());
    assert_eq!(error.unwrap(), 0, "creating an empty file needs no space");

    // Truncation frees space.
    file.truncate(6).await.unwrap();
    assert_eq!(file.append(Bytes::from_static(b"xyzw")).await.unwrap(), 6);

    // Raising the capacity mid-simulation lets appends continue.
    disk.set_faults(SimDiskFaults::default());
    file.append(Bytes::from_static(b"more")).await.unwrap();
    assert_eq!(disk.used_bytes(), 14);
}

#[tokio::test]
async fn random_faults_replay_from_the_seed() {
    async fn run(seed: u64) -> (Vec<bool>, Vec<u8>) {
        let faults = SimDiskFaults {
            sync_error_probability: 0.3,
            torn_write_probability: 0.5,
            capacity: None,
        };
        let disk = SimDisk::with_faults(seed, faults);
        let mount = disk.mount();
        let file = mount.create("seg").await.unwrap();
        while mount.sync_dir().await.is_err() {}
        let mut outcomes = Vec::new();
        for round in 0..20u8 {
            file.append(Bytes::from(vec![round; 8])).await.unwrap();
            outcomes.push(file.sync_data().await.is_ok());
        }
        file.append(Bytes::from(vec![0xff; 64])).await.unwrap();
        disk.crash();
        let contents = read_all(&disk.mount().open("seg").await.unwrap()).await;
        (outcomes, contents)
    }

    let first = run(11).await;
    assert_eq!(run(11).await, first);
    assert!(first.0.contains(&true) && first.0.contains(&false));
    let mut differs = false;
    for seed in 12..20 {
        differs |= run(seed).await != first;
    }
    assert!(differs, "every seed produced the same faults");
}

#[tokio::test]
async fn debug_output_names_the_state() {
    let disk = SimDisk::new(0);
    assert!(format!("{disk:?}").starts_with("SimDisk { files: 0"));
    let mount = disk.mount();
    assert!(format!("{mount:?}").contains("SimMount"));
    let file = mount.create("seg").await.unwrap();
    assert_eq!(format!("{file:?}"), "SimFile { id: 0, incarnation: 0 }");
    assert_eq!(disk.file_info("missing"), None);
}
