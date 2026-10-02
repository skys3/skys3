//! Tests of the segment log on simulated and real disks: appends, reads,
//! group commit, segment rollover, recovery, and failures.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use harness::{
    Audit, AuditedMount, delete, extent, inline_put, open, run_workload, runtime, small_config,
};
use skys3_io::{
    BlockingPool, Clock, Disk, MonoTime, RealDisk, SegmentFile, SimDisk, SimDiskFaults,
};
use skys3_log::record::{DecodeError, ErrorClass};
use skys3_log::segment::file_name;
use skys3_log::{
    LogConfig, LogError, LogRecord, RETIRE_GRACE, RecordLocation, RecoveryError, SegmentClass,
    SegmentId, SegmentInfo, SegmentLog, SegmentScanner, SegmentSummary,
};
use tokio::time::Instant;

/// Runs `test` on a paused current-thread runtime.
fn run<F: Future<Output = ()>>(test: F) {
    runtime().block_on(test);
}

async fn append_all<D: Disk>(log: &SegmentLog<D>, records: &[LogRecord]) -> Vec<RecordLocation> {
    let mut locations = Vec::new();
    for record in records {
        locations.push(log.append(record).await.unwrap());
    }
    locations
}

fn class_of<D: Disk>(log: &SegmentLog<D>, id: SegmentId) -> SegmentClass {
    log.segments().iter().find(|s| s.id == id).unwrap().class
}

#[test]
fn appends_read_back_from_their_class_of_segment() {
    run(async {
        let disk = SimDisk::new(1);
        let (log, report) = open(disk.mount(), LogConfig::default()).await.unwrap();
        assert_eq!(report, Default::default());
        assert!(log.segments().is_empty());

        let records = [
            delete(0, 1),
            extent(1, 2, 1 << 20),
            inline_put(2, 3, 1000),
            extent(1, 4, 10),
        ];
        let locations = append_all(&log, &records).await;
        for (record, location) in records.iter().zip(&locations) {
            assert_eq!(&log.read(*location).await.unwrap(), record);
            assert_eq!(
                class_of(&log, location.segment),
                SegmentClass::of(record.kind())
            );
            assert_eq!(location.len as usize, record.to_bytes().unwrap().len());
        }
        // Records of a class follow each other in its segment.
        assert_eq!(locations[0].offset, 0);
        assert_eq!(locations[2].offset, locations[0].end());
        assert_eq!(locations[1].offset, 0);
        assert_eq!(locations[3].offset, locations[1].end());

        let segments = log.segments();
        assert_eq!(
            segments,
            [
                SegmentInfo {
                    id: SegmentId::new(0),
                    class: SegmentClass::Hot,
                    len: locations[2].end(),
                },
                SegmentInfo {
                    id: SegmentId::new(1),
                    class: SegmentClass::Bulk,
                    len: locations[3].end(),
                },
            ]
        );
        assert!(disk.file_info("hot-0000000000000000.seg").is_some());
        assert!(disk.file_info("bulk-0000000000000001.seg").is_some());

        let stats = log.stats();
        assert_eq!(stats.records, 4);
        assert_eq!(stats.group_commits, 4);
        assert_eq!(stats.bytes, locations[2].end() + locations[3].end());
        assert!((stats.records_per_commit() - 1.0).abs() < f64::EPSILON);
        assert!(log.is_in_service());
        assert!(log.failure().is_none());
        assert!(format!("{log:?}").contains("SegmentLog"));
    });
}

#[test]
fn concurrent_appends_share_group_commits() {
    run(async {
        let disk = SimDisk::new(2);
        let config = LogConfig {
            group_commit_max_delay: Duration::from_millis(1),
            ..LogConfig::default()
        };
        let (log, _) = open(disk.mount(), config).await.unwrap();
        let tasks: Vec<_> = (0..64)
            .map(|i| {
                let log = log.clone();
                tokio::spawn(async move { log.append(&delete(i % 4, u64::from(i))).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        let stats = log.stats();
        assert_eq!(stats.records, 64);
        assert_eq!(stats.group_commits, 1, "{stats:?}");
        assert!((stats.records_per_commit() - 64.0).abs() < f64::EPSILON);
    });
}

#[test]
fn a_group_waits_at_most_the_max_delay() {
    run(async {
        let delay = Duration::from_millis(3);
        let config = LogConfig {
            group_commit_max_delay: delay,
            ..LogConfig::default()
        };
        let (log, _) = open(SimDisk::new(3).mount(), config).await.unwrap();
        let start = Instant::now();
        let late = {
            let log = log.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                log.append(&delete(0, 2)).await
            })
        };
        log.append(&delete(0, 1)).await.unwrap();
        assert_eq!(start.elapsed(), delay);
        // A record that arrives while the group waits joins it.
        late.await.unwrap().unwrap();
        assert_eq!(start.elapsed(), delay);
        assert_eq!(log.stats().group_commits, 1);

        // Timers have a resolution of one millisecond, so shorter delays
        // round up.
        let delay = Duration::from_micros(500);
        let config = LogConfig {
            group_commit_max_delay: delay,
            ..LogConfig::default()
        };
        let (log, _) = open(SimDisk::new(3).mount(), config).await.unwrap();
        let start = Instant::now();
        log.append(&delete(0, 1)).await.unwrap();
        assert!(
            (delay..=Duration::from_millis(1)).contains(&start.elapsed()),
            "{:?}",
            start.elapsed()
        );

        let config = LogConfig {
            group_commit_max_delay: Duration::ZERO,
            ..LogConfig::default()
        };
        let (log, _) = open(SimDisk::new(3).mount(), config).await.unwrap();
        let start = Instant::now();
        log.append(&delete(0, 1)).await.unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
    });
}

#[test]
fn a_full_group_commits_without_waiting() {
    run(async {
        let config = LogConfig {
            group_commit_max_delay: Duration::from_secs(3600),
            group_commit_max_bytes: 4096,
            ..LogConfig::default()
        };
        let (log, _) = open(SimDisk::new(4).mount(), config).await.unwrap();
        let start = Instant::now();
        // One record larger than the group limit commits on its own.
        log.append(&extent(0, 1, 8192)).await.unwrap();
        // Records that fill a group together commit together.
        let tasks: Vec<_> = (0..4)
            .map(|i| {
                let log = log.clone();
                tokio::spawn(async move { log.append(&extent(0, 10 + i, 2100)).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        let stats = log.stats();
        assert_eq!(stats.records, 5);
        assert_eq!(stats.group_commits, 3, "{stats:?}");
    });
}

/// The delay bounds how long a group waits, not which queued records it
/// takes: records that queued behind a slow sync all join the next group
/// at once, even those that arrived after its first record's deadline.
#[test]
fn records_queued_behind_a_slow_sync_join_the_next_group_without_waiting() {
    run(async {
        let disk = SimDisk::new(21);
        let plan = Audit {
            sync_delay: Duration::from_millis(5),
            ..Audit::default()
        };
        let config = LogConfig {
            group_commit_max_delay: Duration::from_millis(1),
            ..small_config()
        };
        let (log, _) = open(AuditedMount::new(&disk, plan), config).await.unwrap();
        let start = Instant::now();
        let append_at = |at_ms: u64, seq: u64| {
            let log = log.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(at_ms)).await;
                let location = log.append(&delete(0, seq)).await.unwrap();
                (location, start.elapsed())
            })
        };
        // A's group closes at 1 ms, and its syncs take until 6 ms. B and C
        // queue meanwhile; C arrives after B's deadline of 3 ms. D arrives
        // to an idle log.
        let tasks = [
            append_at(0, 1),
            append_at(2, 2),
            append_at(4, 3),
            append_at(20, 4),
        ];
        let mut acks = Vec::new();
        for task in tasks {
            acks.push(task.await.unwrap());
        }
        let ms = Duration::from_millis;
        let times: Vec<_> = acks.iter().map(|(_, at)| *at).collect();
        assert_eq!(times, [ms(6), ms(11), ms(11), ms(26)]);
        let [_, (b, _), (c, _), _] = acks[..] else {
            unreachable!()
        };
        assert_eq!(c.segment, b.segment);
        assert_eq!(c.offset, b.end());
        assert_eq!(log.stats().group_commits, 3);
    });
}

#[test]
fn segments_roll_over_between_group_commits() {
    run(async {
        let disk = SimDisk::new(5);
        let mount = AuditedMount::new(&disk, Audit::default());
        let config = LogConfig {
            group_commit_max_delay: Duration::ZERO,
            ..small_config()
        };
        let (log, _) = open(mount.clone(), config.clone()).await.unwrap();
        let mut acknowledged = Vec::new();
        for seq in 0..40 {
            let record = if seq % 3 == 0 {
                extent(0, seq, 700)
            } else {
                inline_put(0, seq, 300)
            };
            let location = log.append(&record).await.unwrap();
            acknowledged.push((record, location));
        }
        mount.check_acknowledged(&acknowledged);

        let segments = log.segments();
        for class in SegmentClass::ALL {
            let of_class: Vec<_> = segments.iter().filter(|s| s.class == class).collect();
            assert!(of_class.len() > 2, "{class}: {of_class:?}");
            for segment in &of_class {
                assert!(segment.len <= config.segment_bytes, "{segment:?}");
            }
        }
        // Ids increase in creation order across classes.
        let ids: Vec<_> = segments.iter().map(|s| s.id.get()).collect();
        assert_eq!(ids, (0..segments.len() as u64).collect::<Vec<_>>());
        for (record, location) in &acknowledged {
            assert_eq!(&log.read(*location).await.unwrap(), record);
        }
    });
}

#[test]
fn a_group_larger_than_a_segment_gets_a_segment_of_its_own() {
    run(async {
        let config = LogConfig {
            group_commit_max_delay: Duration::ZERO,
            segment_bytes: 1000,
            ..small_config()
        };
        let (log, _) = open(SimDisk::new(6).mount(), config).await.unwrap();
        let small = log.append(&delete(0, 1)).await.unwrap();
        let big = log.append(&extent(0, 2, 3000)).await.unwrap();
        let after = log.append(&extent(0, 3, 10)).await.unwrap();
        assert_eq!(big.offset, 0);
        assert!(big.len > 1000);
        assert_ne!(after.segment, big.segment);
        assert_eq!(after.offset, 0);
        assert_eq!(small.segment, SegmentId::new(0));
    });
}

#[test]
fn reopening_recovers_every_record_and_continues_the_segments() {
    run(async {
        let disk = SimDisk::new(7);
        let config = small_config();
        let (log, _) = open(disk.mount(), config.clone()).await.unwrap();
        let outcome = run_workload(&log, 3, 12).await;
        assert_eq!(outcome.failed, 0);
        let before = log.segments();
        drop(log);
        disk.crash();

        let (log, report) = open(disk.mount(), config).await.unwrap();
        assert_eq!(report.segments, before);
        assert!(report.torn_tails.is_empty());
        assert!(report.ignored_files.is_empty());
        harness::check_recovered(&log, &outcome).await;

        // New records follow the recovered ones in the last segment of
        // their class, and new segments take fresh ids.
        let last_hot = before
            .iter()
            .rev()
            .find(|s| s.class == SegmentClass::Hot)
            .unwrap();
        let location = log.append(&delete(9, 1)).await.unwrap();
        if last_hot.len + u64::from(location.len) <= 2048 {
            assert_eq!(location.segment, last_hot.id);
            assert_eq!(location.offset, last_hot.len);
        } else {
            assert_eq!(location.segment.get(), before.last().unwrap().id.get() + 1);
        }
    });
}

/// Writes two records to a fresh disk, then appends `tail` to the hot
/// segment directly and syncs it, as a torn or damaged write would leave
/// it. Returns the disk and the records' locations.
async fn disk_with_tail(config: &LogConfig, tail: &[u8]) -> (SimDisk, Vec<RecordLocation>) {
    let disk = SimDisk::new(8);
    let (log, _) = open(disk.mount(), config.clone()).await.unwrap();
    let locations = append_all(&log, &[delete(0, 1), delete(0, 2)]).await;
    drop(log);
    let file = disk
        .mount()
        .open(&file_name(SegmentClass::Hot, SegmentId::new(0)))
        .await
        .unwrap();
    file.append(Bytes::copy_from_slice(tail)).await.unwrap();
    file.sync_data().await.unwrap();
    (disk, locations)
}

#[test]
fn recovery_cuts_torn_tails() {
    run(async {
        let config = small_config();
        let whole = delete(0, 3).to_bytes().unwrap();
        let mut bad_crc = whole.to_vec();
        *bad_crc.last_mut().unwrap() ^= 1;
        let cases: [(&[u8], ErrorClass); 5] = [
            (&whole[..whole.len() - 1], ErrorClass::Incomplete),
            (&whole[..10], ErrorClass::Incomplete),
            (&whole[..2], ErrorClass::Incomplete),
            (&[0; 100], ErrorClass::Corrupt),
            (&bad_crc, ErrorClass::Corrupt),
        ];
        for (tail, class) in cases {
            let (disk, locations) = disk_with_tail(&config, tail).await;
            let (log, report) = open(disk.mount(), config.clone()).await.unwrap();
            let [torn] = &report.torn_tails[..] else {
                panic!("expected one torn tail: {report:?}");
            };
            assert_eq!(torn.segment, SegmentId::new(0));
            assert_eq!(torn.valid_len, locations[1].end());
            assert_eq!(torn.cut_bytes, tail.len() as u64);
            assert_eq!(torn.reason.class(), class, "{:?}", torn.reason);
            assert_eq!(report.segments[0].len, locations[1].end());
            assert_eq!(log.read(locations[1]).await.unwrap(), delete(0, 2));

            // The cut is durable, and new records follow the valid ones.
            let next = log.append(&delete(0, 3)).await.unwrap();
            assert_eq!(next.offset, locations[1].end());
            drop(log);
            disk.crash();
            let (log, report) = open(disk.mount(), config.clone()).await.unwrap();
            assert!(report.torn_tails.is_empty());
            assert_eq!(log.read(next).await.unwrap(), delete(0, 3));
        }
    });
}

/// Re-seals a record after its bytes were changed.
fn reseal(record: &mut [u8]) {
    let crc = crc32c::crc32c(&record[8..]);
    record[4..8].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn recovery_refuses_records_it_cannot_use() {
    run(async {
        let config = small_config();
        let whole = delete(0, 3).to_bytes().unwrap().to_vec();
        let mut newer_version = whole.clone();
        newer_version[8] = 3;
        reseal(&mut newer_version);
        let mut reserved_kind = whole.clone();
        reserved_kind[10] = 10; // PART_FLUSHED, not defined yet
        reseal(&mut reserved_kind);
        let mut broken_header = whole.clone();
        broken_header[22] = 1; // reserved byte
        reseal(&mut broken_header);
        let cases = [
            (newer_version, ErrorClass::Unsupported),
            (reserved_kind, ErrorClass::Unsupported),
            (broken_header, ErrorClass::Invalid),
        ];
        for (tail, class) in cases {
            let (disk, locations) = disk_with_tail(&config, &tail).await;
            let error = open(disk.mount(), config.clone()).await.unwrap_err();
            let RecoveryError::Unreadable {
                segment,
                offset,
                source,
            } = &error
            else {
                panic!("unexpected error: {error}");
            };
            assert_eq!(*segment, SegmentId::new(0));
            assert_eq!(*offset, locations[1].end());
            assert_eq!(source.class(), class);
            assert!(error.to_string().contains("cannot use"), "{error}");
            // Nothing was cut.
            let info = disk.file_info("hot-0000000000000000.seg").unwrap();
            assert_eq!(info.written, locations[1].end() + tail.len() as u64);
        }
    });
}

#[test]
fn recovery_refuses_damage_followed_by_synced_records() {
    run(async {
        let config = LogConfig {
            group_commit_max_bytes: 1024,
            ..LogConfig::default()
        };
        let window = config.tear_window();
        let disk = SimDisk::new(9);
        let (log, _) = open(disk.mount(), config.clone()).await.unwrap();
        let mut locations = Vec::new();
        let mut written = 0;
        let mut seq = 0;
        while written < window + (2 << 20) {
            let location = log.append(&extent(0, seq, 1 << 20)).await.unwrap();
            written = location.end();
            locations.push(location);
            seq += 1;
        }
        drop(log);

        // Damage the first record: far more than a group commit follows it.
        let name = file_name(SegmentClass::Bulk, SegmentId::new(0));
        let damage = |disk: &SimDisk, at: u64| {
            let disk = disk.clone();
            let name = name.clone();
            async move {
                let mount = disk.mount();
                let file = mount.open(&name).await.unwrap();
                let len = file.len();
                let rest = file.read_at(at, (len - at) as usize).await.unwrap();
                file.truncate(at).await.unwrap();
                let mut rest = rest.to_vec();
                rest[20] ^= 0xff;
                file.append(rest.into()).await.unwrap();
                file.sync_data().await.unwrap();
            }
        };
        damage(&disk, locations[0].offset).await;
        let error = open(disk.mount(), config.clone()).await.unwrap_err();
        let RecoveryError::Damaged {
            segment,
            offset,
            source,
            next_valid,
        } = &error
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(*segment, SegmentId::new(0));
        assert_eq!(*offset, 0);
        assert_eq!(source.class(), ErrorClass::Corrupt);
        assert!(*next_valid >= window, "{next_valid}");
        assert!(locations.iter().any(|l| l.offset == *next_valid));
        assert!(error.to_string().contains("damaged"), "{error}");

        // Damage within a group commit of the end is a torn tail: the
        // records after it were never acknowledged.
        let disk = SimDisk::new(10);
        let (log, _) = open(disk.mount(), config.clone()).await.unwrap();
        let locations = append_all(&log, &[extent(0, 1, 1000), extent(0, 2, 1000)]).await;
        drop(log);
        damage(&disk, locations[0].offset).await;
        let (_, report) = open(disk.mount(), config.clone()).await.unwrap();
        assert_eq!(report.torn_tails.len(), 1);
        assert_eq!(report.torn_tails[0].valid_len, 0);
        assert_eq!(report.torn_tails[0].cut_bytes, locations[1].end());
    });
}

#[test]
fn recovery_reports_other_files_and_refuses_duplicate_ids() {
    run(async {
        let disk = SimDisk::new(11);
        let mount = disk.mount();
        mount.create("index.redb").await.unwrap();
        mount.create("hot-00000000000000AA.seg").await.unwrap();
        let (log, report) = open(mount.clone(), small_config()).await.unwrap();
        assert_eq!(
            report.ignored_files,
            ["hot-00000000000000AA.seg", "index.redb"]
        );
        assert!(log.segments().is_empty());
        drop(log);

        mount
            .create(&file_name(SegmentClass::Hot, SegmentId::new(3)))
            .await
            .unwrap();
        mount
            .create(&file_name(SegmentClass::Bulk, SegmentId::new(3)))
            .await
            .unwrap();
        let error = open(mount, small_config()).await.unwrap_err();
        assert!(matches!(error, RecoveryError::DuplicateId(id) if id == SegmentId::new(3)));
        assert!(error.to_string().contains("both"), "{error}");
    });
}

#[test]
fn recovery_fails_on_io_errors() {
    run(async {
        let disk = SimDisk::new(12);
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        log.append(&delete(0, 1)).await.unwrap();
        drop(log);
        // The directory sync at the end of recovery fails.
        disk.fail_next_syncs(2);
        let error = open(disk.mount(), small_config()).await.unwrap_err();
        assert!(matches!(error, RecoveryError::Io(_)), "{error}");
        assert!(error.to_string().contains("injected"), "{error}");
    });
}

#[test]
fn a_failed_sync_takes_the_disk_out_of_service() {
    run(async {
        let disk = SimDisk::new(13);
        let config = LogConfig {
            group_commit_max_delay: Duration::from_millis(1),
            ..small_config()
        };
        let (log, _) = open(disk.mount(), config.clone()).await.unwrap();
        let acknowledged = log.append(&delete(0, 1)).await.unwrap();

        disk.fail_next_syncs(1);
        let tasks: Vec<_> = (2..10)
            .map(|seq| {
                let log = log.clone();
                tokio::spawn(async move { log.append(&delete(0, seq)).await })
            })
            .collect();
        for task in tasks {
            let error = task.await.unwrap().unwrap_err();
            assert!(matches!(error, LogError::OutOfService(_)), "{error}");
        }
        assert!(!log.is_in_service());
        let failure = log.failure().unwrap();
        assert_eq!(failure.to_string(), "injected sync error");

        // Nothing is appended or read any more, and a later sync that
        // would succeed is never tried.
        let error = log.append(&delete(0, 99)).await.unwrap_err();
        assert!(error.to_string().contains("out of service"), "{error}");
        assert!(matches!(
            log.read(acknowledged).await,
            Err(LogError::OutOfService(_))
        ));
        assert!(matches!(
            log.scan(acknowledged.segment),
            Err(LogError::OutOfService(_))
        ));
        assert_eq!(log.stats().records, 1);

        // After a restart, the acknowledged record is there and the
        // records whose sync failed are not.
        drop(log);
        disk.crash();
        let (log, report) = open(disk.mount(), config).await.unwrap();
        assert_eq!(log.read(acknowledged).await.unwrap(), delete(0, 1));
        assert_eq!(report.segments[0].len, acknowledged.end());
    });
}

#[test]
fn records_queued_behind_a_failed_sync_are_never_acknowledged() {
    run(async {
        let disk = SimDisk::new(20);
        let config = LogConfig {
            group_commit_max_delay: Duration::ZERO,
            ..small_config()
        };
        let plan = Audit {
            // The first sync of a group commit; recovery's directory sync
            // on the empty disk is sync 0.
            fail_sync_at: Some(1),
            ..Audit::default()
        };
        let mount = AuditedMount::new(&disk, plan);
        let (log, _) = open(mount.clone(), config).await.unwrap();
        let first = {
            let log = log.clone();
            tokio::spawn(async move { log.append(&delete(0, 1)).await })
        };
        // Wait until the group commit is under way, then queue more.
        let ops = mount.audit().ops;
        while mount.audit().ops == ops {
            tokio::task::yield_now().await;
        }
        let queued: Vec<_> = (2..6)
            .map(|seq| {
                let log = log.clone();
                tokio::spawn(async move { log.append(&delete(0, seq)).await })
            })
            .collect();
        assert!(first.await.unwrap().is_err());
        for task in queued {
            let error = task.await.unwrap().unwrap_err();
            assert!(matches!(error, LogError::OutOfService(_)), "{error}");
        }
        assert!(mount.audit().sync_failed);
        assert_eq!(log.stats().group_commits, 0);
    });
}

#[test]
fn a_full_disk_takes_the_disk_out_of_service() {
    run(async {
        let disk = SimDisk::with_faults(
            14,
            SimDiskFaults {
                capacity: Some(3000),
                ..SimDiskFaults::default()
            },
        );
        let (log, _) = open(disk.mount(), LogConfig::default()).await.unwrap();
        log.append(&extent(0, 1, 1000)).await.unwrap();
        let error = log.append(&extent(0, 2, 5000)).await.unwrap_err();
        let LogError::OutOfService(cause) = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(cause.kind(), io::ErrorKind::StorageFull);
    });
}

#[test]
fn exhausted_segment_ids_take_the_disk_out_of_service() {
    run(async {
        let disk = SimDisk::new(15);
        let last = SegmentId::new(u64::MAX);
        disk.mount()
            .create(&file_name(SegmentClass::Hot, last))
            .await
            .unwrap();
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        // The hot class has a segment; the bulk class needs a new one.
        let hot = log.append(&delete(0, 1)).await.unwrap();
        assert_eq!(hot.segment, last);
        let error = log.append(&extent(0, 2, 10)).await.unwrap_err();
        assert!(error.to_string().contains("exhausted"), "{error}");
    });
}

#[test]
fn appends_check_their_records() {
    run(async {
        let config = LogConfig {
            inline_max_bytes: 100,
            ..small_config()
        };
        let (log, _) = open(SimDisk::new(16).mount(), config).await.unwrap();
        // `check` refuses what `append` refuses, without appending.
        let checked = log.check(&inline_put(0, 1, 101)).unwrap_err();
        assert!(matches!(checked, LogError::InlineTooLarge { .. }));
        let error = log.append(&inline_put(0, 1, 101)).await.unwrap_err();
        assert!(matches!(
            error,
            LogError::InlineTooLarge { len: 101, max: 100 }
        ));
        log.check(&inline_put(0, 1, 100)).unwrap();
        log.check(&extent(0, 2, 1000)).unwrap();
        assert!(error.to_string().contains("inline_max_bytes"), "{error}");
        log.append(&inline_put(0, 1, 100)).await.unwrap();
        log.append(&extent(0, 2, 1000)).await.unwrap();

        let mut bad = delete(0, 3);
        bad.body = skys3_log::RecordBody::Delete(skys3_log::record::Delete { key: String::new() });
        assert!(matches!(log.check(&bad), Err(LogError::Encode(_))));
        assert!(matches!(log.append(&bad).await, Err(LogError::Encode(_))));

        // Encoded records are verified, classified, and checked the same way.
        let encoded = extent(0, 4, 500).to_bytes().unwrap();
        let location = log.append_encoded(encoded.clone()).await.unwrap();
        assert_eq!(log.read(location).await.unwrap(), extent(0, 4, 500));
        let big = inline_put(0, 5, 101).to_bytes().unwrap();
        assert!(matches!(
            log.append_encoded(big).await,
            Err(LogError::InlineTooLarge { .. })
        ));
        let truncated = encoded.slice(..encoded.len() - 1);
        let error = log.append_encoded(truncated).await.unwrap_err();
        assert!(matches!(error, LogError::InvalidRecord(_)), "{error}");
        let mut padded = encoded.to_vec();
        padded.push(0);
        let error = log.append_encoded(padded.into()).await.unwrap_err();
        assert!(
            matches!(error, LogError::InvalidRecord(DecodeError::Malformed(_))),
            "{error}"
        );
        assert!(error.to_string().contains("unexpected bytes"), "{error}");
    });
}

#[test]
fn reads_check_their_locations() {
    run(async {
        let (log, _) = open(SimDisk::new(17).mount(), small_config())
            .await
            .unwrap();
        let [first, second] = append_all(&log, &[delete(0, 1), delete(0, 2)]).await[..] else {
            unreachable!()
        };
        let unknown = RecordLocation {
            segment: SegmentId::new(40),
            ..first
        };
        assert!(matches!(
            log.read(unknown).await,
            Err(LogError::UnknownSegment(_))
        ));
        assert!(matches!(
            log.scan(SegmentId::new(40)),
            Err(LogError::UnknownSegment(_))
        ));
        let past_end = RecordLocation {
            offset: second.end(),
            ..second
        };
        let error = log.read(past_end).await.unwrap_err();
        assert!(matches!(error, LogError::Read { .. }), "{error}");
        let misaligned = RecordLocation {
            offset: first.offset + 1,
            ..first
        };
        let error = log.read(misaligned).await.unwrap_err();
        assert!(matches!(error, LogError::Damaged { .. }), "{error}");
        assert!(error.to_string().contains("damaged"), "{error}");
        let too_long = RecordLocation {
            len: first.len + second.len,
            ..first
        };
        let error = log.read(too_long).await.unwrap_err();
        assert!(matches!(error, LogError::WrongLength { .. }), "{error}");
        assert!(error.to_string().contains("bytes long"), "{error}");
    });
}

#[test]
fn scans_return_records_in_order() {
    run(async {
        let (log, _) = open(SimDisk::new(18).mount(), LogConfig::default())
            .await
            .unwrap();
        // Extents larger than a scanner's read-ahead chunk.
        let records: Vec<_> = (0..5).map(|seq| extent(0, seq, 700_000)).collect();
        let locations = append_all(&log, &records).await;
        let mut scanner = log.scan(locations[0].segment).unwrap();
        assert_eq!(scanner.segment(), locations[0].segment);
        assert_eq!(scanner.end(), locations[4].end());
        for (record, location) in records.iter().zip(&locations) {
            let scanned = scanner.next().await.unwrap().unwrap();
            assert_eq!(scanned.location, *location);
            assert_eq!(&scanned.decode().unwrap(), record);
            assert_eq!(scanned.header.kind, record.kind());
            assert_eq!(scanner.offset(), location.end());
        }
        assert!(scanner.next().await.unwrap().is_none());
    });
}

#[test]
fn a_log_whose_runtime_is_gone_is_closed() {
    let first = runtime();
    let (log, _) = first
        .block_on(open(SimDisk::new(19).mount(), small_config()))
        .unwrap();
    drop(first);
    let error = runtime().block_on(log.append(&delete(0, 1))).unwrap_err();
    assert!(matches!(error, LogError::Closed), "{error}");
    assert_eq!(error.to_string(), "the log is closed");
}

#[test]
fn a_real_disk_keeps_records_across_reopening() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let pool = BlockingPool::new("log-disk", NonZeroUsize::new(2).unwrap()).unwrap();
        let disk = RealDisk::open(dir.path(), pool.clone()).await.unwrap();
        let (log, _) = open(disk.clone(), small_config()).await.unwrap();
        let outcome = run_workload(&log, 4, 10).await;
        assert_eq!(outcome.failed, 0);
        drop(log);

        // A torn write at the end of the newest hot segment.
        let hot = disk
            .list()
            .await
            .unwrap()
            .into_iter()
            .rfind(|name| name.starts_with("hot-"))
            .unwrap();
        let file = disk.open(&hot).await.unwrap();
        let len = file.len();
        file.append(Bytes::from_static(b"SKYL\x01")).await.unwrap();
        drop(file);

        let disk = RealDisk::open(dir.path(), pool).await.unwrap();
        let (log, report) = open(disk, small_config()).await.unwrap();
        assert_eq!(report.torn_tails.len(), 1);
        assert_eq!(report.torn_tails[0].valid_len, len);
        harness::check_recovered(&log, &outcome).await;
        let location = log.append(&delete(0, 1)).await.unwrap();
        assert_eq!(log.read(location).await.unwrap(), delete(0, 1));
    });
}

#[test]
fn summaries_cover_acknowledged_records_since_the_log_opened() {
    run(async {
        let disk = SimDisk::new(11);
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        assert!(log.summaries().is_empty());
        let records = [delete(1, 5), extent(2, 3, 100), delete(1, 4), delete(2, 9)];
        let locations = append_all(&log, &records).await;
        let summaries = log.summaries();
        let hot = &summaries[&locations[0].segment];
        assert_eq!(hot.start, 0);
        assert_eq!(hot.end, locations[3].end());
        assert_eq!(
            hot.positions,
            BTreeMap::from([
                (harness::shard(1), records[0].position),
                (harness::shard(2), records[3].position),
            ])
        );
        let bulk = &summaries[&locations[1].segment];
        assert_eq!(bulk.end, locations[1].end());
        assert!(bulk.is_behind(|_| Some(records[1].position)));
        assert!(!bulk.is_behind(|_| None));
        drop(log);
        disk.crash();

        // After a restart, a recovered segment's summary starts at its
        // recovered length.
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        let summaries = log.summaries();
        let hot = &summaries[&locations[0].segment];
        assert_eq!(
            (hot.start, hot.end),
            (locations[3].end(), locations[3].end())
        );
        assert!(hot.positions.is_empty());
        let next = log.append(&delete(3, 1)).await.unwrap();
        let hot = &log.summaries()[&next.segment];
        assert_eq!(hot.end, next.end());

        // Released segments leave the summaries; unknown ids are ignored.
        log.release([locations[1].segment, SegmentId::new(999)]);
        assert_eq!(log.released(), BTreeSet::from([locations[1].segment]));
        assert!(!log.summaries().contains_key(&locations[1].segment));
    });
}

#[test]
fn summaries_merge_runs_that_meet_or_overlap() {
    let shard = harness::shard(1);
    let position = |seq| delete(1, seq).position;
    let mut first = SegmentSummary::starting_at(0);
    first.add(&shard, position(4), 100);
    first.add(&shard, position(2), 150);
    assert_eq!(first.positions[&shard], position(4));
    let mut gap = SegmentSummary::starting_at(200);
    gap.add(&shard, position(9), 300);
    assert!(!first.clone().merge(&gap));
    let mut next = SegmentSummary::starting_at(150);
    next.add(&shard, position(7), 220);
    assert!(first.merge(&next));
    assert_eq!((first.start, first.end), (0, 220));
    assert_eq!(first.positions[&shard], position(7));
    // An empty run that ends further still extends the summary.
    assert!(first.merge(&SegmentSummary::starting_at(220)));
    assert!(first.merge(&SegmentSummary {
        start: 100,
        end: 400,
        positions: BTreeMap::new(),
    }));
    assert_eq!(first.end, 400);
    assert!(!first.merge(&SegmentSummary::starting_at(500)));
}

#[test]
fn scan_range_reads_part_of_a_segment() {
    run(async {
        let disk = SimDisk::new(12);
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        let records = [delete(1, 1), delete(1, 2), delete(1, 3)];
        let locations = append_all(&log, &records).await;
        let segment = locations[0].segment;
        let scan = |from, to| {
            let log = log.clone();
            async move {
                let mut scanner = log.scan_range(segment, from, to).unwrap();
                let mut found = Vec::new();
                while let Some(record) = scanner.next().await.unwrap() {
                    found.push(record.location);
                }
                (found, scanner.end())
            }
        };
        let (found, end) = scan(locations[1].offset, u64::MAX).await;
        assert_eq!(
            (found.as_slice(), end),
            (&locations[1..], locations[2].end())
        );
        let (found, _) = scan(0, locations[1].end()).await;
        assert_eq!(found, &locations[..2]);
        let (found, end) = scan(u64::MAX, u64::MAX).await;
        assert_eq!((found.len(), end), (0, locations[2].end()));
        assert!(matches!(
            log.scan_range(SegmentId::new(77), 0, 1),
            Err(LogError::UnknownSegment(_))
        ));
    });
}

#[test]
fn lazy_records_ride_the_next_group_commit() {
    run(async {
        let disk = SimDisk::new(9);
        let (log, _) = open(disk.mount(), LogConfig::default()).await.unwrap();

        // A lazy record joins the group the next ordinary record starts, in
        // queue order, and costs no sync of its own.
        let lazy = tokio::spawn({
            let log = log.clone();
            async move { log.append_lazy(&delete(0, 1)).await.unwrap() }
        });
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!lazy.is_finished());
        assert_eq!(log.stats().group_commits, 0);
        let eager = log.append(&delete(0, 2)).await.unwrap();
        let lazy = lazy.await.unwrap();
        assert_eq!(lazy.offset, 0);
        assert_eq!(eager.offset, lazy.end());
        assert_eq!(log.stats().group_commits, 1);
        assert_eq!(log.stats().records, 2);

        // On an idle disk it commits on its own after LAZY_MAX_DELAY.
        let started = Instant::now();
        let alone = log.append_lazy(&delete(0, 3)).await.unwrap();
        assert!(started.elapsed() >= skys3_log::LAZY_MAX_DELAY);
        assert_eq!(alone.offset, eager.end());
        assert_eq!(log.stats().group_commits, 2);
        assert_eq!(log.read(alone).await.unwrap(), delete(0, 3));

        // Asked for, it commits at once, in a group of its own.
        let started = Instant::now();
        let asked = tokio::spawn({
            let log = log.clone();
            async move { log.append_lazy(&delete(0, 4)).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!asked.is_finished());
        log.commit_lazy_now();
        let asked = asked.await.unwrap();
        assert!(started.elapsed() < skys3_log::LAZY_MAX_DELAY);
        assert_eq!(asked.offset, alone.end());
        assert_eq!(log.stats().group_commits, 3);
    });
}

/// A hook a [`HookedClock`] runs on each reading.
type Hook = Box<dyn Fn() + Send>;

/// The test clock, which runs a hook on every reading while one is set:
/// a way to look at the log from inside a call at each point where it
/// reads the clock.
#[derive(Default)]
struct HookedClock {
    hook: Mutex<Option<Hook>>,
}

impl std::fmt::Debug for HookedClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookedClock").finish_non_exhaustive()
    }
}

impl HookedClock {
    fn set_hook(&self, hook: Option<Hook>) {
        *self.hook.lock().unwrap() = hook;
    }
}

impl Clock for HookedClock {
    fn now(&self) -> MonoTime {
        if let Some(hook) = &*self.hook.lock().unwrap() {
            hook();
        }
        harness::clock().now()
    }

    fn runtime_deadline(&self, deadline: MonoTime) -> Instant {
        harness::clock().runtime_deadline(deadline)
    }
}

/// Appends records until the log has a hot segment besides its last one,
/// releases it, and returns its id and the records in it.
async fn released_segment<D: Disk>(log: &SegmentLog<D>) -> (SegmentId, Vec<LogRecord>) {
    let records: Vec<_> = (1..=6).map(|seq| inline_put(0, seq, 400)).collect();
    let locations = append_all(log, &records).await;
    let first = locations[0].segment;
    assert_ne!(locations[5].segment, first, "the log rolled over");
    log.release([first]);
    let held = records
        .into_iter()
        .zip(&locations)
        .filter(|(_, location)| location.segment == first)
        .map(|(record, _)| record)
        .collect();
    (first, held)
}

async fn scan_records<F: SegmentFile>(mut scanner: SegmentScanner<F>) -> Vec<LogRecord> {
    let mut records = Vec::new();
    while let Some(scanned) = scanner.next().await.unwrap() {
        records.push(scanned.decode().unwrap());
    }
    records
}

#[test]
fn a_segment_being_retired_is_always_readable() {
    run(async {
        let disk = SimDisk::new(10);
        let clock = Arc::new(HookedClock::default());
        let (log, _) = SegmentLog::open(disk.mount(), small_config(), clock.clone())
            .await
            .unwrap();
        let (id, held) = released_segment(&log).await;
        // Retiring reads the clock. At each reading, wherever it falls, the
        // segment must be found, listed or retired.
        let seen = Arc::new(Mutex::new(Vec::new()));
        clock.set_hook(Some(Box::new({
            let (log, seen) = (log.clone(), Arc::clone(&seen));
            move || seen.lock().unwrap().push(log.scan(id).map(|_| ()))
        })));
        log.retire(id).await.unwrap();
        clock.set_hook(None);
        let seen = std::mem::take(&mut *seen.lock().unwrap());
        assert!(!seen.is_empty());
        assert!(seen.iter().all(Result::is_ok), "{seen:?}");
        assert_eq!(scan_records(log.scan(id).unwrap()).await, held);
    });
}

#[test]
fn listed_segments_stay_scannable_after_they_are_retired() {
    run(async {
        let disk = SimDisk::new(11);
        let (log, _) = open(disk.mount(), small_config()).await.unwrap();
        let (id, held) = released_segment(&log).await;
        let listed: Vec<_> = log.segments().iter().map(|s| s.id).collect();
        let pinned = log.scan_all().unwrap();
        let scanned: Vec<_> = pinned.iter().map(SegmentScanner::segment).collect();
        assert_eq!(scanned, listed);
        log.retire(id).await.unwrap();
        assert!(!log.segments().iter().any(|s| s.id == id));

        // Within the grace, a scan of a segment listed before finds every
        // record in it.
        assert_eq!(scan_records(log.scan(id).unwrap()).await, held);
        let end = log.scan(id).unwrap().end();
        assert_eq!(
            scan_records(log.scan_range(id, 0, end).unwrap()).await,
            held
        );

        // After it, only scanners taken before still read the segment.
        tokio::time::advance(RETIRE_GRACE).await;
        assert_eq!(log.drop_retired(), 1);
        assert!(matches!(log.scan(id), Err(LogError::UnknownSegment(s)) if s == id));
        for scanner in pinned {
            let segment = scanner.segment();
            let records = scan_records(scanner).await;
            if segment == id {
                assert_eq!(records, held);
            }
        }
    });
}
