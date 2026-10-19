//! Compaction of fragment segments (§10.3): live fragments are copied
//! under their IDs and their segments removed; reclaimed and damaged ones
//! are not copied, and the last segment is never compacted.

mod support;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use skys3_ec::fragment::{BLOCK_LEN, FragmentHeader, FragmentRecord};
use skys3_ec::{CompactionReport, FragmentError, FragmentId, FragmentStore};
use skys3_io::disk::SimFile;
use skys3_io::{Disk, SegmentFile, SimDisk, SimMount};
use support::{fragment, runtime, small_config};

async fn open(mount: SimMount) -> FragmentStore<SimMount> {
    FragmentStore::open(mount, small_config()).await.unwrap().0
}

/// Writes `count` fragments of `len` bytes, keyed `prefix/n`.
async fn write(
    store: &FragmentStore<SimMount>,
    prefix: &str,
    count: u64,
    len: u64,
) -> Vec<(FragmentId, FragmentHeader, Bytes)> {
    let mut written = Vec::new();
    for n in 0..count {
        let (header, payload) = fragment(&format!("{prefix}/{n}"), len, n);
        let id = store.write(&header, payload.clone()).await.unwrap();
        written.push((id, header, payload));
    }
    written
}

async fn check(store: &FragmentStore<SimMount>, fragments: &[(FragmentId, FragmentHeader, Bytes)]) {
    for (id, header, payload) in fragments {
        let read = store.read(*id, 0..payload.len() as u64).await.unwrap();
        assert_eq!((&read.header, &read.data), (header, payload), "{id}");
    }
}

#[test]
fn live_fragments_move_under_their_ids_and_dead_segments_go() {
    runtime().block_on(async {
        let disk = SimDisk::new(1);
        let store = open(disk.mount()).await;
        // 64 KiB fragments in 256 KiB segments: three to a segment.
        let all = write(&store, "a", 10, 65_536).await;
        let segments = store.segments();
        assert!(segments.len() >= 4, "{segments:?}");
        // Reclaim two of every three: the sealed segments fall below half.
        let (mut kept, mut reclaimed) = (Vec::new(), Vec::new());
        for (n, fragment) in all.into_iter().enumerate() {
            if n % 3 == 0 {
                kept.push(fragment);
            } else {
                store.reclaim(fragment.0).await.unwrap();
                reclaimed.push(fragment.0);
            }
        }
        let last = segments.last().unwrap().id;
        let report = store.compact(0.5).await.unwrap();
        assert_eq!(report.segments, segments.len() - 1, "{report:?}");
        assert_eq!(report.damaged, 0);
        assert!(report.copied >= 2, "{report:?}");
        assert!(report.reclaimed_bytes() > 0, "{report:?}");
        let after = store.segments();
        assert!(after.iter().all(|segment| segment.id >= last), "{after:?}");
        check(&store, &kept).await;
        for id in &reclaimed {
            assert!(store.len(*id).is_none());
        }
        // Nothing is left to compact; the last segment never is.
        assert_eq!(
            store.compact(1.0).await.unwrap(),
            CompactionReport::default()
        );

        // A restart finds the copies, and only the live fragments.
        drop(store);
        disk.crash();
        let store = open(disk.mount()).await;
        check(&store, &kept).await;
        let mut ids: Vec<FragmentId> = kept.iter().map(|(id, ..)| *id).collect();
        ids.sort();
        assert_eq!(store.ids(), ids);
    });
}

#[test]
fn segments_mostly_live_stay_where_they_are() {
    runtime().block_on(async {
        let store = open(SimDisk::new(2).mount()).await;
        let all = write(&store, "b", 7, 65_536).await;
        let before = store.segments();
        // One of three reclaimed in the first segment: two thirds live.
        store.reclaim(all[0].0).await.unwrap();
        let report = store.compact(0.5).await.unwrap();
        assert_eq!(report.segments, 0, "{report:?}");
        assert_eq!(store.segments(), before);
        // A higher threshold takes it.
        let report = store.compact(0.9).await.unwrap();
        assert_eq!((report.segments, report.copied), (1, 2), "{report:?}");
        check(&store, &all[1..]).await;
    });
}

#[test]
fn damaged_fragments_are_dropped_not_copied() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let mount = disk.mount();
        let len = 2 * BLOCK_LEN;
        let record = |key: &str, id: u128, seed: u64| {
            let (header, payload) = fragment(key, len, seed);
            let id = FragmentId::new(id);
            let bytes = FragmentRecord {
                id,
                header: header.clone(),
                payload: payload.clone(),
            }
            .to_bytes()
            .unwrap();
            (id, header, payload, bytes)
        };
        let good = record("good", 0, 1);
        let rotten = record("rotten", good.3.len() as u128, 2);
        let mut damaged = rotten.3.to_vec();
        let at = damaged.len() - 1;
        damaged[at] ^= 1;
        let gone = record("gone", 2 * good.3.len() as u128, 4);
        // A sealed segment holding a good fragment, a damaged one, and one
        // to reclaim, then a last segment.
        let file = mount.create("frag-0000000000000000.seg").await.unwrap();
        file.append(good.3.clone()).await.unwrap();
        file.append(Bytes::from(damaged)).await.unwrap();
        file.append(gone.3.clone()).await.unwrap();
        file.sync_data().await.unwrap();
        let next = record("next", 1 << 64, 3);
        let last = mount.create("frag-0000000000000001.seg").await.unwrap();
        last.append(next.3.clone()).await.unwrap();
        last.sync_data().await.unwrap();
        mount.sync_dir().await.unwrap();

        let store = open(disk.mount()).await;
        store.reclaim(gone.0).await.unwrap();
        assert!(matches!(
            store.read(rotten.0, 0..len).await,
            Err(FragmentError::Damaged { .. })
        ));
        let report = store.compact(1.0).await.unwrap();
        assert_eq!(
            (report.segments, report.copied, report.damaged),
            (1, 1, 1),
            "{report:?}"
        );
        assert!(store.len(rotten.0).is_none());
        let read = store.read(good.0, 0..len).await.unwrap();
        assert_eq!((read.header, read.data), (good.1, good.2));
        // Recovery checks the last segment's payloads: the copy is sound.
        drop(store);
        disk.crash();
        let store = open(disk.mount()).await;
        assert_eq!(store.ids(), [good.0, next.0]);
    });
}

/// A mount whose file removals fail while `failing` is set.
#[derive(Clone, Debug)]
struct FailRemoves {
    mount: SimMount,
    failing: Arc<AtomicBool>,
}

impl Disk for FailRemoves {
    type File = SimFile;

    async fn create(&self, name: &str) -> io::Result<SimFile> {
        self.mount.create(name).await
    }

    async fn open(&self, name: &str) -> io::Result<SimFile> {
        self.mount.open(name).await
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(io::Error::other("the removal failed"));
        }
        self.mount.remove(name).await
    }

    async fn list(&self) -> io::Result<Vec<String>> {
        self.mount.list().await
    }

    async fn sync_dir(&self) -> io::Result<()> {
        self.mount.sync_dir().await
    }
}

#[test]
fn a_segment_whose_file_was_not_removed_is_compacted_again() {
    runtime().block_on(async {
        let disk = SimDisk::new(5);
        let failing = Arc::new(AtomicBool::new(false));
        let mount = FailRemoves {
            mount: disk.mount(),
            failing: Arc::clone(&failing),
        };
        let store = FragmentStore::open(mount, small_config()).await.unwrap().0;
        let ids = |store: &FragmentStore<FailRemoves>| -> Vec<u64> {
            store.segments().iter().map(|segment| segment.id).collect()
        };
        let files = || async { disk.mount().list().await.unwrap().len() };
        // Three 64 KiB fragments to a segment: segments 0 to 2.
        let mut all = Vec::new();
        for n in 0..7 {
            let (header, payload) = fragment(&format!("d/{n}"), 65_536, n);
            let id = store.write(&header, payload.clone()).await.unwrap();
            all.push((id, header, payload));
        }
        let before = ids(&store);
        assert_eq!(before.len(), 3, "{before:?}");

        // Compaction copies the live fragment of segment 0, then fails to
        // remove its file: the segment stays listed, for the next pass.
        store.reclaim(all[1].0).await.unwrap();
        store.reclaim(all[2].0).await.unwrap();
        failing.store(true, Ordering::Relaxed);
        let error = store.compact(0.5).await.unwrap_err();
        assert!(matches!(error, FragmentError::Remove { .. }), "{error}");
        assert!(ids(&store).contains(&before[0]), "{:?}", ids(&store));
        // So does a segment that reclaiming empties.
        for (id, ..) in &all[3..6] {
            let _ = store.reclaim(*id).await;
        }
        assert!(ids(&store).contains(&before[1]), "{:?}", ids(&store));
        let listed = files().await;

        failing.store(false, Ordering::Relaxed);
        let report = store.compact(0.5).await.unwrap();
        assert_eq!(report.segments, 2, "{report:?}");
        assert_eq!(files().await, listed - 2);
        let after = ids(&store);
        assert!(!after.contains(&before[0]) && !after.contains(&before[1]));
        for (id, header, payload) in [&all[0], &all[6]] {
            let read = store.read(*id, 0..payload.len() as u64).await.unwrap();
            assert_eq!((&read.header, &read.data), (header, payload));
        }
    });
}

#[test]
fn a_copy_that_fails_leaves_the_segment_and_loses_nothing() {
    runtime().block_on(async {
        let disk = SimDisk::new(4);
        let store = open(disk.mount()).await;
        let all = write(&store, "c", 4, 65_536).await;
        store.reclaim(all[1].0).await.unwrap();
        store.reclaim(all[2].0).await.unwrap();
        let ids = |store: &FragmentStore<SimMount>| -> Vec<u64> {
            store.segments().iter().map(|segment| segment.id).collect()
        };
        let before = ids(&store);
        disk.fail_next_syncs(1);
        let error = store.compact(0.5).await.unwrap_err();
        assert!(matches!(error, FragmentError::OutOfService(_)), "{error}");
        assert!(!store.is_in_service());
        assert_eq!(ids(&store), before);
        assert!(matches!(
            store.compact(0.5).await,
            Err(FragmentError::OutOfService(_))
        ));

        // The next life finds every live fragment where it was.
        drop(store);
        disk.crash();
        let store = open(disk.mount()).await;
        check(&store, &[all[0].clone(), all[3].clone()]).await;
    });
}
