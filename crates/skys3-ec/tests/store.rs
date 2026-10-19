//! The fragment store: writes, verified range reads, and recovery's
//! handling of torn tails, damage, and records it cannot use.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use skys3_ec::fragment::{
    BLOCK_LEN, FragmentDecodeError, FragmentRecord, MAX_FRAGMENT_LEN, MAX_HEADER_LEN,
};
use skys3_ec::{FragmentError, FragmentId, FragmentStore, FragmentStoreConfig, RecoveryError};
use skys3_io::{Disk, MonotonicClock, SegmentFile, SimDisk, SimMount};
use skys3_log::record::{Delete, ErrorClass};
use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentLog};
use skys3_types::ETag;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm};
use support::{fragment, header, position, runtime, shard, small_config};

/// The ID of a fragment first written to `disk`, `segment`, at `offset`.
fn id(disk: u8, segment: u64, offset: u64) -> FragmentId {
    FragmentId::new(u128::from(disk) << 120 | u128::from(segment) << 64 | u128::from(offset))
}

/// The encoded record of a fragment of `len` bytes with ID `id`.
fn record(id: FragmentId, key: &str, len: u64, seed: u64) -> Bytes {
    let (header, payload) = fragment(key, len, seed);
    FragmentRecord {
        id,
        header,
        payload,
    }
    .to_bytes()
    .unwrap()
}

/// Writes fragment segment `segment` holding `parts`, synced.
async fn write_segment(mount: &SimMount, segment: u64, parts: &[&[u8]]) {
    let file = mount
        .create(&format!("frag-{segment:016x}.seg"))
        .await
        .unwrap();
    for part in parts {
        file.append(Bytes::copy_from_slice(part)).await.unwrap();
    }
    file.sync_data().await.unwrap();
    mount.sync_dir().await.unwrap();
}

async fn open(mount: SimMount) -> FragmentStore<SimMount> {
    FragmentStore::open(mount, small_config()).await.unwrap().0
}

#[test]
fn reads_ranges_verified_against_block_checksums() {
    runtime().block_on(async {
        let disk = SimDisk::new(1);
        let store = open(disk.mount()).await;
        let len = 3 * BLOCK_LEN + 640;
        let (header, payload) = fragment("photos/cat.jpg", len, 7);
        let id = store.write(&header, payload.clone()).await.unwrap();
        assert_eq!(id, self::id(0, 0, 0));
        assert_eq!(store.len(id), Some(len));
        assert_eq!(store.header(id).await.unwrap(), header);

        let ranges = [
            0..len,
            0..1,
            BLOCK_LEN - 3..BLOCK_LEN + 3,
            BLOCK_LEN..2 * BLOCK_LEN,
            len - 1..len,
            2 * BLOCK_LEN + 5..len,
            17..17,
        ];
        for range in ranges {
            let read = store.read(id, range.clone()).await.unwrap();
            let expected = &payload[range.start as usize..range.end as usize];
            assert_eq!(read.data, expected, "{range:?}");
            assert_eq!(read.crc32c, crc32c::crc32c(expected));
            assert_eq!(read.header, header);
        }

        let (second, payload) = fragment("photos/dog.jpg", 64, 8);
        let next = store.write(&second, payload).await.unwrap();
        assert!(next > id);
        assert_eq!(store.ids(), [id, next]);
        assert_eq!(store.segments().len(), 1);
        assert!(store.is_in_service());
        assert!(format!("{store:?}").contains("fragments: 2"));
    });
}

#[test]
fn rejects_bad_requests() {
    runtime().block_on(async {
        let store = open(SimDisk::new(2).mount()).await;
        let (header, payload) = fragment("k", 4096, 1);
        let id = store.write(&header, payload).await.unwrap();

        let missing = self::id(0, 9, 9);
        assert!(matches!(
            store.read(missing, 0..1).await,
            Err(FragmentError::UnknownFragment(m)) if m == missing
        ));
        assert!(matches!(
            store.header(missing).await,
            Err(FragmentError::UnknownFragment(_))
        ));
        assert_eq!(store.len(missing), None);
        for range in [0..4097, 4097..4097] {
            let error = store.read(id, range).await.unwrap_err();
            assert!(
                matches!(error, FragmentError::OutOfRange { len: 4096, .. }),
                "{error}"
            );
        }
        #[expect(clippy::reversed_empty_ranges, reason = "the store must reject it")]
        let backwards = 9..3;
        assert!(matches!(
            store.read(id, backwards).await,
            Err(FragmentError::OutOfRange { .. })
        ));

        // The payload must be the stripe's fragment length.
        let error = store
            .write(&header, Bytes::from(vec![0; 4032]))
            .await
            .unwrap_err();
        assert!(matches!(error, FragmentError::Encode(_)), "{error}");
        // The store has a limit of its own.
        let (big, payload) = fragment("big", 2 << 20, 1);
        let error = store.write(&big, payload).await.unwrap_err();
        assert!(
            matches!(error, FragmentError::TooLarge { max: 1_048_576, .. }),
            "{error}"
        );
        // A header that breaks the format.
        let mut bad = header.clone();
        bad.index = 6;
        let error = store
            .write(&bad, Bytes::from(vec![0; 4096]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("index"), "{error}");
    });
}

#[test]
fn fragments_survive_restarts_across_segments() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let store = open(disk.mount()).await;
        let mut written = Vec::new();
        for n in 0..12 {
            let (header, payload) = fragment(&format!("k{n}"), 65_536 + 64 * n, n);
            let id = store.write(&header, payload.clone()).await.unwrap();
            written.push((id, header, payload));
        }
        let segments = store.segments();
        assert!(segments.len() > 2, "{segments:?}");
        // Every segment but the last stays near `segment_bytes`.
        for segment in &segments[..segments.len() - 1] {
            assert!(
                segment.len <= 2 * small_config().segment_bytes,
                "{segment:?}"
            );
        }
        drop(store);
        disk.crash();

        let (store, report) = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap();
        assert_eq!(report.fragments, written.len());
        assert_eq!(report.segments, segments);
        assert_eq!(report.torn_tail, None);
        for (id, header, payload) in &written {
            let read = store.read(*id, 0..payload.len() as u64).await.unwrap();
            assert_eq!((&read.header, &read.data), (header, payload));
        }
        // New fragments get new IDs.
        let (header, payload) = fragment("after", 64, 99);
        let id = store.write(&header, payload).await.unwrap();
        assert!(written.iter().all(|(other, ..)| *other < id));
    });
}

#[test]
fn shares_a_disk_with_the_log_without_touching_its_segments() {
    runtime().block_on(async {
        let disk = SimDisk::new(4);
        let clock = Arc::new(MonotonicClock::new());
        let (log, _) = SegmentLog::open(disk.mount(), LogConfig::default(), clock.clone())
            .await
            .unwrap();
        let entry = LogRecord {
            shard: shard(),
            position: position(4, 1),
            body: RecordBody::Delete(Delete { key: "k".into() }),
        };
        let location = log.append(&entry).await.unwrap();
        let store = open(disk.mount()).await;
        let (header, payload) = fragment("k", 64, 1);
        let id = store.write(&header, payload).await.unwrap();
        drop((log, store));
        disk.crash();

        let (log, log_report) = SegmentLog::open(disk.mount(), LogConfig::default(), clock)
            .await
            .unwrap();
        let (store, report) = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap();
        assert_eq!(log_report.ignored_files, ["frag-0000000000000000.seg"]);
        assert_eq!(report.ignored_files, ["hot-0000000000000000.seg"]);
        // Compaction works on the segments the log lists, which are its own.
        assert_eq!(log.segments().len(), 1);
        assert_eq!(log.read(location).await.unwrap(), entry);
        assert_eq!(store.ids(), [id]);
    });
}

#[test]
fn recovery_cuts_torn_tails() {
    runtime().block_on(async {
        let first = record(id(0, 0, 0), "a", 4096, 1);
        let next = record(id(0, 0, first.len() as u64), "b", 65_600, 2);
        let tails: [&[u8]; 4] = [
            // A fixed header cut short.
            &next[..20],
            // A header without its payload.
            &next[..next.len() - 1],
            // Garbage where a record should start.
            &[0xab; 300],
            // A whole header whose payload's last block was torn.
            &[&next[..next.len() - 1], &[0][..]].concat(),
        ];
        for (n, tail) in tails.into_iter().enumerate() {
            let disk = SimDisk::new(5);
            let mount = disk.mount();
            write_segment(&mount, 0, &[&first, tail]).await;
            let (store, report) = FragmentStore::open(disk.mount(), small_config())
                .await
                .unwrap();
            let torn = report.torn_tail.expect("a torn tail");
            assert_eq!(torn.segment, 0);
            assert_eq!(torn.valid_len, first.len() as u64, "{n}");
            assert_eq!(torn.cut_bytes, tail.len() as u64, "{n}");
            assert!(
                matches!(
                    torn.reason.class(),
                    ErrorClass::Incomplete | ErrorClass::Corrupt
                ),
                "{n}: {}",
                torn.reason
            );
            assert_eq!(store.ids(), [id(0, 0, 0)]);
            // The next fragment goes where the torn one was.
            let (header, payload) = fragment("c", 64, 3);
            let written = store.write(&header, payload).await.unwrap();
            assert_eq!(written, id(0, 0, first.len() as u64));
        }
    });
}

#[test]
fn recovery_refuses_damage_and_records_it_cannot_use() {
    runtime().block_on(async {
        let good = record(id(0, 0, 0), "a", 4096, 1);
        // A bad record in a segment that is not the last.
        let disk = SimDisk::new(6);
        let mount = disk.mount();
        write_segment(&mount, 0, &[&good, &[0; 64]]).await;
        write_segment(&mount, 1, &[&record(id(0, 1, 0), "b", 64, 2)]).await;
        let error = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                RecoveryError::Damaged {
                    segment: 0,
                    offset: 4_096..,
                    next_valid: None,
                    ..
                }
            ),
            "{error}"
        );

        // In the last segment, a bad record followed by a good one beyond
        // the tear window.
        let config = FragmentStoreConfig {
            max_fragment_bytes: 4096,
            group_commit_max_bytes: 4096,
            ..small_config()
        };
        let gap = vec![0xee; (config.tear_window() + 1) as usize];
        let disk = SimDisk::new(7);
        let late = record(id(0, 0, 9), "c", 64, 3);
        write_segment(&disk.mount(), 0, &[&good, &gap, &late]).await;
        let error = FragmentStore::open(disk.mount(), config.clone())
            .await
            .unwrap_err();
        let expected = (good.len() + gap.len()) as u64;
        assert!(
            matches!(error, RecoveryError::Damaged { next_valid: Some(at), .. } if at == expected),
            "{error}"
        );
        // Within the window, the same bytes are a torn tail.
        let disk = SimDisk::new(8);
        let gap = vec![0xee; 100];
        write_segment(&disk.mount(), 0, &[&good, &gap, &late]).await;
        let (_, report) = FragmentStore::open(disk.mount(), config).await.unwrap();
        assert_eq!(report.torn_tail.unwrap().valid_len, good.len() as u64);

        // A record of a newer format version.
        let mut newer = good.to_vec();
        newer[8] = 2;
        let crc = crc32c::crc32c(&newer[8..u32_at(&newer, 12) as usize]);
        newer[4..8].copy_from_slice(&crc.to_le_bytes());
        let disk = SimDisk::new(9);
        write_segment(&disk.mount(), 0, &[&newer]).await;
        let error = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                RecoveryError::Unreadable {
                    source: FragmentDecodeError::UnsupportedVersion(2),
                    ..
                }
            ),
            "{error}"
        );

        // A malformed header under a valid CRC.
        let mut malformed = good.to_vec();
        malformed[10] = 1;
        let crc = crc32c::crc32c(&malformed[8..u32_at(&malformed, 12) as usize]);
        malformed[4..8].copy_from_slice(&crc.to_le_bytes());
        let disk = SimDisk::new(10);
        write_segment(&disk.mount(), 0, &[&malformed]).await;
        let error = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap_err();
        assert!(matches!(error, RecoveryError::Unreadable { .. }), "{error}");
        assert!(error.to_string().contains("reserved"), "{error}");
    });
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

#[test]
fn a_disk_opened_under_another_number_is_refused() {
    runtime().block_on(async {
        let disk = SimDisk::new(11);
        let store = open(disk.mount()).await;
        let (header, payload) = fragment("k", 64, 1);
        store.write(&header, payload).await.unwrap();
        drop(store);
        let config = FragmentStoreConfig {
            disk: 1,
            ..small_config()
        };
        let error = FragmentStore::open(disk.mount(), config).await.unwrap_err();
        assert!(
            matches!(
                error,
                RecoveryError::ForeignFragment {
                    disk: 0,
                    segment: 0,
                    ..
                }
            ),
            "{error}"
        );
        // A store on disk 1 assigns IDs of disk 1.
        let config = FragmentStoreConfig {
            disk: 1,
            ..small_config()
        };
        let (store, _) = FragmentStore::open(SimDisk::new(12).mount(), config)
            .await
            .unwrap();
        let (header, payload) = fragment("k", 64, 1);
        assert_eq!(store.write(&header, payload).await.unwrap(), id(1, 0, 0));
    });
}

#[test]
fn reads_detect_damaged_payloads_and_prefer_later_copies() {
    runtime().block_on(async {
        let len = 2 * BLOCK_LEN;
        let original = record(id(0, 0, 0), "a", len, 1);
        let mut rotten = original.to_vec();
        // A flipped bit in the payload's second block.
        let last = rotten.len() - 1;
        rotten[last] ^= 1;
        let other = record(id(0, 0, original.len() as u64), "b", 64, 2);
        let disk = SimDisk::new(13);
        // Recovery checks payloads only in the last segment.
        write_segment(&disk.mount(), 0, &[&rotten, &other]).await;
        write_segment(&disk.mount(), 1, &[&record(id(0, 1, 0), "c", 64, 3)]).await;
        let store = open(disk.mount()).await;
        let damaged = id(0, 0, 0);
        assert_eq!(
            store.read(damaged, 0..BLOCK_LEN).await.unwrap().data.len(),
            64 * 1024
        );
        let error = store
            .read(damaged, BLOCK_LEN - 1..BLOCK_LEN + 1)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                FragmentError::Damaged {
                    source: FragmentDecodeError::BlockMismatch { block: 1, .. },
                    ..
                }
            ),
            "{error}"
        );
        drop(store);

        // A later copy of the same fragment, as compaction makes, wins.
        write_segment(&disk.mount(), 2, &[&original]).await;
        let store = open(disk.mount()).await;
        assert_eq!(
            store.read(damaged, 0..len).await.unwrap().data.len(),
            len as usize
        );
        assert_eq!(store.ids().len(), 3);
    });
}

#[test]
fn a_failed_sync_takes_the_store_out_of_service() {
    runtime().block_on(async {
        let disk = SimDisk::new(14);
        let store = open(disk.mount()).await;
        let (header, payload) = fragment("k", 64, 1);
        let id = store.write(&header, payload.clone()).await.unwrap();
        disk.fail_next_syncs(1);
        let error = store.write(&header, payload.clone()).await.unwrap_err();
        assert!(matches!(error, FragmentError::OutOfService(_)), "{error}");
        assert!(!store.is_in_service());
        // Nothing is acknowledged or read after it.
        let error = store.write(&header, payload).await.unwrap_err();
        assert!(matches!(error, FragmentError::OutOfService(_)), "{error}");
        assert!(matches!(
            store.read(id, 0..1).await,
            Err(FragmentError::OutOfService(_))
        ));
        assert!(error.to_string().contains("out of service"));
    });
}

/// The largest header every bounded field allows fits in
/// `MAX_HEADER_LEN`, with the block checksums of the largest fragment.
#[test]
fn the_largest_header_fits_the_format() {
    let (mut header, payload) = fragment("k", 64, 1);
    header.key = "k".repeat(1024);
    header.object = support::multipart(1, 10_000);
    header.object.etag = ETag::new("e".repeat(ETag::MAX_LEN)).unwrap();
    header.object.tags = (0..50)
        .map(|n| (format!("{n:0>512}"), "v".repeat(1024)))
        .collect();
    // Many short metadata entries cost the most framing.
    let names = (b'a'..=b'z')
        .flat_map(|a| (b'a'..=b'z').map(move |b| format!("{}{}", a as char, b as char)));
    let mut total = 0;
    for name in names {
        if total + name.len() > 8192 {
            break;
        }
        total += name.len();
        header.object.metadata.insert(name, String::new());
    }
    header.object.checksums = ChecksumAlgorithm::ALL
        .into_iter()
        .map(|algorithm| {
            let digest = vec![7; algorithm.digest_len()];
            (
                algorithm,
                Checksum::full_object(algorithm, &digest).unwrap(),
            )
        })
        .collect();
    header.stripe.data_len = 256;
    header.object.size = 10_000;
    let bytes = FragmentRecord {
        id: id(0, 0, 0),
        header,
        payload,
    }
    .to_bytes()
    .unwrap();
    let header_len = u64::from(u32_at(&bytes, 12));
    let blocks = 4 * (MAX_FRAGMENT_LEN / BLOCK_LEN);
    assert!(
        header_len + blocks <= u64::from(MAX_HEADER_LEN),
        "{header_len}"
    );
}

#[test]
fn headers_name_their_version() {
    let header = header("k", 64, 0);
    let version = header.version_identity();
    assert_eq!(version.seq, header.version.seq);
    assert_eq!(version.etag, header.object.etag);
}
