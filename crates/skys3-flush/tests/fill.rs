//! Read-through fill (§9.2): evicted versions read from the remote with
//! their conditions, committed as extents, and made clean (§4.2, Evicted →
//! Clean); concurrent reads coalesced; and `ADOPT` when the remote changed
//! out of band, dropped when a local write came first.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_flush::{Counters, FILL_CHUNK_BYTES, FillBody, FillError, Filler, FlushSettings};
use skys3_index::{Entry, EntryState, Payload};
use skys3_io::ManualWallClock;
use skys3_log::RecordBody;
use skys3_log::record::Import;
use skys3_remote::{DeleteObject, ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use skys3_types::{ETag, EpochSeq};
use support::{Node, Patience, md5_etag, remote, runtime, settings, target};

/// A fill's settings: 1 KiB extents, unless the test says otherwise.
fn fill_settings(extent_bytes: u64) -> FlushSettings {
    FlushSettings {
        extent_bytes,
        ..settings()
    }
}

/// A filler from `store`, counting in `counters`.
fn filler(store: &SimS3, extent_bytes: u64, counters: &Counters) -> Filler<SimS3> {
    Filler::new(
        Arc::new(store.clone()),
        "",
        fill_settings(extent_bytes),
        counters.clone(),
        Arc::new(ManualWallClock::new(Duration::from_secs(1_750_000_000))),
    )
}

/// Writes `body` to `key` at the remote as another writer would, and
/// returns its ETag.
async fn write_remote(store: &SimS3, key: &str, body: impl Into<Bytes>) -> ETag {
    let mut metadata = UserMetadata::new();
    metadata.insert("writer", "someone-else").unwrap();
    let request = PutObject::new(key, body.into())
        .with_metadata(metadata)
        .with_content_type("text/plain");
    store.put_object(request).await.unwrap().etag
}

/// Commits an `IMPORT` of `key`, a remote object of `size` bytes with
/// `etag`, and returns the stub's version.
async fn import(node: &Node, key: &str, size: u64, etag: ETag) -> EpochSeq {
    let body = RecordBody::Import(Import {
        key: key.to_owned(),
        size,
        last_modified_ms: 1_600_000_000_000,
        etag,
        storage_class: None,
    });
    let committed = node.shard.commit(body).await.unwrap();
    assert!(committed.outcome.is_applied());
    committed.position
}

/// Reads all of `body`.
async fn collect(mut body: FillBody) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.recv().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

/// Reads `range` of `version` of `key` through `filler`, all of it.
async fn read(
    filler: &Filler<SimS3>,
    node: &Node,
    key: &str,
    version: EpochSeq,
    range: std::ops::Range<u64>,
) -> Result<Vec<u8>, FillError> {
    let body = filler.read(&node.shard, key, version, range).await?;
    Ok(collect(body).await.expect("the fill streams the range"))
}

/// Waits until `key`'s entry satisfies `done`, and returns it.
async fn wait_for(node: &Node, key: &str, done: impl Fn(&Entry) -> bool) -> Entry {
    let patience = Patience::new();
    loop {
        if let Some(entry) = node.entry(key).await
            && done(&entry)
        {
            return entry;
        }
        assert!(!patience.is_exhausted(), "{key} never got there");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `len` bytes of a pattern.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[test]
fn an_evicted_version_is_filled_and_becomes_clean() {
    runtime().block_on(async {
        let node = Node::open(1).await;
        let store = remote(1, false);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let etag = write_remote(&store, "k", "hello, world").await;
        let stub = import(&node, "k", 12, etag).await;

        // A range is served while the fill streams the rest.
        let bytes = read(&filler, &node, "k", stub, 3..9).await.unwrap();
        assert_eq!(bytes, b"lo, wo");
        let entry = wait_for(&node, "k", |e| e.state == EntryState::Clean).await;
        assert_eq!(entry.version, stub, "a fill makes no new version");
        let Payload::Extents(extents) = entry.object.unwrap().payload else {
            panic!("a fill commits extents");
        };
        let lens: Vec<u32> = extents.iter().map(|e| e.len).collect();
        assert_eq!(lens, [4, 4, 4]);
        let mut cached = Vec::new();
        for extent in &extents {
            cached.extend_from_slice(&node.shard.payload(extent.position).await.unwrap());
        }
        assert_eq!(cached, b"hello, world");
        assert_eq!(counters.fills.get(), 1);
        assert_eq!(counters.fill_conflicts.get(), 0);

        // The version is clean now: a read that named it evicted resolves
        // the key again and finds the bytes local.
        assert_eq!(
            read(&filler, &node, "k", stub, 0..12).await,
            Err(FillError::Changed)
        );
        assert_eq!(counters.fills.get(), 1);
        assert!(format!("{filler:?}").contains("Filler"));
    });
}

#[test]
fn a_large_object_is_filled_in_chunks_of_extents() {
    runtime().block_on(async {
        let node = Node::open(2).await;
        let store = remote(2, false);
        let filler = filler(&store, 1 << 20, &Counters::default());
        let size = FILL_CHUNK_BYTES as usize + 5000;
        let body = pattern(size);
        let etag = write_remote(&store, "big", body.clone()).await;
        let stub = import(&node, "big", size as u64, etag).await;

        let requests = store.stats().requests;
        let start = FILL_CHUNK_BYTES - 10;
        let bytes = read(&filler, &node, "big", stub, start..start + 20)
            .await
            .unwrap();
        assert_eq!(bytes, &body[start as usize..start as usize + 20]);
        let entry = wait_for(&node, "big", |e| e.state == EntryState::Clean).await;
        let Payload::Extents(extents) = entry.object.unwrap().payload else {
            panic!("a fill commits extents");
        };
        assert_eq!(extents.len(), 9, "eight whole extents and the rest");
        assert_eq!(store.stats().requests - requests, 2, "two ranged GETs");
    });
}

#[test]
fn an_empty_object_is_filled() {
    runtime().block_on(async {
        let node = Node::open(3).await;
        let store = remote(3, false);
        let filler = filler(&store, 4, &Counters::default());
        let etag = write_remote(&store, "empty", Bytes::new()).await;
        let stub = import(&node, "empty", 0, etag).await;
        assert_eq!(
            read(&filler, &node, "empty", stub, 0..0).await,
            Ok(Vec::new())
        );
        let entry = wait_for(&node, "empty", |e| e.state == EntryState::Clean).await;
        assert_eq!(entry.object.unwrap().payload, Payload::Extents(Vec::new()));
    });
}

#[test]
fn concurrent_reads_share_one_fill() {
    runtime().block_on(async {
        let node = Node::open(4).await;
        let store = remote(4, false);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let etag = write_remote(&store, "k", "shared bytes").await;
        let stub = import(&node, "k", 12, etag).await;
        // The first GET takes a while, so the second read joins it.
        store.inject(
            Operation::GetObject,
            Fault::Delay(Duration::from_millis(200)),
        );
        let requests = store.stats().requests;
        let (first, second) = tokio::join!(
            read(&filler, &node, "k", stub, 0..12),
            read(&filler, &node, "k", stub, 7..12),
        );
        assert_eq!(first.unwrap(), b"shared bytes");
        assert_eq!(second.unwrap(), b"bytes");
        assert_eq!(store.stats().requests - requests, 1, "one GET");
        wait_for(&node, "k", |e| e.state == EntryState::Clean).await;
        assert_eq!(counters.fills.get(), 1);
    });
}

#[test]
fn an_out_of_band_write_is_adopted_and_read_next() {
    runtime().block_on(async {
        let node = Node::open(5).await;
        let store = remote(5, false);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let etag = write_remote(&store, "k", "ours").await;
        let stub = import(&node, "k", 4, etag.clone()).await;
        let theirs = write_remote(&store, "k", "their write").await;

        assert_eq!(
            read(&filler, &node, "k", stub, 0..4).await,
            Err(FillError::Changed)
        );
        assert_eq!(counters.fill_conflicts.get(), 1);
        let adopted = node.entry("k").await.unwrap();
        assert!(adopted.version > stub, "the adopted version is new");
        assert_eq!(adopted.state, EntryState::Evicted);
        assert_eq!(adopted.remote_etag.as_ref(), Some(&theirs));
        let object = adopted.object.unwrap();
        assert_eq!((object.size, &object.local_etag), (11, &theirs));
        assert_eq!(object.metadata["x-amz-meta-writer"], "someone-else");
        assert_eq!(object.metadata["content-type"], "text/plain");
        assert_eq!(
            object.last_modified_ms, 1_750_000_000_000,
            "the remote gave no Last-Modified"
        );

        // The next read fills the adopted version.
        let bytes = read(&filler, &node, "k", adopted.version, 0..11).await;
        assert_eq!(bytes.unwrap(), b"their write");
        assert_eq!(counters.fill_conflicts.get(), 1);
    });
}

#[test]
fn a_local_write_during_the_fill_wins_over_the_adopt() {
    // Real time, so the delayed GET is still in flight while the local
    // write commits.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let node = Node::open(6).await;
        let store = remote(6, false);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let etag = write_remote(&store, "k", "ours").await;
        let stub = import(&node, "k", 4, etag.clone()).await;
        write_remote(&store, "k", "their write").await;

        store.inject(
            Operation::GetObject,
            Fault::Delay(Duration::from_millis(300)),
        );
        let requests = store.stats().requests;
        let reader = {
            let (filler, shard) = (filler.clone(), node.shard.clone());
            tokio::spawn(async move { filler.read(&shard, "k", stub, 0..4).await.map(drop) })
        };
        // Once the GET is at the remote, a client writes the key.
        while store.stats().requests == requests {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let local = node.put("k", "local write").await;

        assert_eq!(reader.await.unwrap(), Err(FillError::Changed));
        assert_eq!(counters.fill_conflicts.get(), 1, "the conflict is counted");
        let entry = node.entry("k").await.unwrap();
        assert_eq!(entry.version.seq.get(), local);
        assert_eq!(entry.state, EntryState::Dirty);
        // The `ADOPT` was dropped: the local version still conditions its
        // flush on the remote object it replaces, and the flusher finds the
        // conflict (§7.2).
        assert_eq!(entry.remote_etag, Some(etag));
        assert_eq!(entry.object.unwrap().local_etag, md5_etag(b"local write"));
    });
}

#[test]
fn a_remote_object_deleted_out_of_band_is_gone() {
    runtime().block_on(async {
        let node = Node::open(7).await;
        let store = remote(7, false);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let etag = write_remote(&store, "k", "ours").await;
        let stub = import(&node, "k", 4, etag).await;
        store.delete_object(DeleteObject::new("k")).await.unwrap();

        assert_eq!(
            read(&filler, &node, "k", stub, 0..4).await,
            Err(FillError::Gone)
        );
        assert_eq!(counters.fill_conflicts.get(), 1);
        let entry = node.entry("k").await.unwrap();
        assert_eq!((entry.version, entry.state), (stub, EntryState::Evicted));
        assert_eq!(
            FillError::Gone.to_string(),
            "the object is gone from the bucket's remote target"
        );
    });
}

#[test]
fn a_versioned_remote_serves_the_named_version_until_it_is_gone() {
    runtime().block_on(async {
        let node = Node::open(8).await;
        let store = remote(8, true);
        let counters = Counters::default();
        let filler = filler(&store, 4, &counters);
        let flusher = node.flusher(&target(&store));
        let seq = node.put("k", "flushed").await;
        node.settle(&flusher).await;
        let clean = node.entry("k").await.unwrap();
        let version_id = clean.remote_version_id.clone().expect("a versioned remote");
        node.shard.evict("k", clean.version).await.unwrap().unwrap();
        let theirs = write_remote(&store, "k", "their write").await;

        // The fill names the version, which the remote still has.
        let bytes = read(&filler, &node, "k", clean.version, 0..7).await;
        assert_eq!(bytes.unwrap(), b"flushed");
        let refilled = wait_for(&node, "k", |e| e.state == EntryState::Clean).await;
        assert_eq!(refilled.version.seq.get(), seq);
        assert_eq!(counters.fill_conflicts.get(), 0);

        // Once that version is gone, the current one is adopted.
        node.shard.evict("k", clean.version).await.unwrap().unwrap();
        let request = DeleteObject::new("k").with_version_id(skys3_remote::VersionId(version_id));
        store.delete_object(request).await.unwrap();
        assert_eq!(
            read(&filler, &node, "k", clean.version, 0..7).await,
            Err(FillError::Changed)
        );
        let adopted = node.entry("k").await.unwrap();
        assert_eq!(adopted.remote_etag, Some(theirs));
        assert!(adopted.remote_version_id.is_some());
        let bytes = read(&filler, &node, "k", adopted.version, 0..11).await;
        assert_eq!(bytes.unwrap(), b"their write");
        assert_eq!(counters.fill_conflicts.get(), 1);
        flusher.stop().await;
    });
}

#[test]
fn transient_errors_are_retried_and_a_failed_fill_is_started_again() {
    runtime().block_on(async {
        let node = Node::open(9).await;
        let store = remote(9, false);
        let filler = filler(&store, 4, &Counters::default());
        let etag = write_remote(&store, "k", "retried").await;
        let stub = import(&node, "k", 7, etag).await;

        store.inject(Operation::GetObject, Fault::InternalError);
        store.inject(Operation::GetObject, Fault::SlowDown);
        let bytes = read(&filler, &node, "k", stub, 0..7).await;
        assert_eq!(bytes.unwrap(), b"retried");
        wait_for(&node, "k", |e| e.state == EntryState::Clean).await;

        node.shard.evict("k", stub).await.unwrap().unwrap();
        for _ in 0..3 {
            store.inject(Operation::GetObject, Fault::InternalError);
        }
        let failed = read(&filler, &node, "k", stub, 0..7).await;
        assert!(
            matches!(&failed, Err(FillError::Failed(reason)) if reason.contains("500")),
            "{failed:?}"
        );
        // The failed fill is forgotten: the next read starts another.
        let bytes = read(&filler, &node, "k", stub, 0..7).await;
        assert_eq!(bytes.unwrap(), b"retried");
    });
}

#[test]
fn a_fill_that_fails_midway_ends_the_streams_reading_it() {
    runtime().block_on(async {
        let node = Node::open(10).await;
        let store = remote(10, false);
        let filler = filler(&store, 1 << 20, &Counters::default());
        let size = FILL_CHUNK_BYTES + 100;
        let etag = write_remote(&store, "big", pattern(size as usize)).await;
        let stub = import(&node, "big", size, etag).await;
        // The first chunk arrives; the second fails every attempt.
        store.inject(Operation::GetObject, Fault::Delay(Duration::ZERO));
        for _ in 0..3 {
            store.inject(Operation::GetObject, Fault::InternalError);
        }
        let body = filler
            .read(&node.shard, "big", stub, 0..size)
            .await
            .unwrap();
        let error = collect(body).await.unwrap_err();
        assert!(error.to_string().contains("the fill failed"), "{error}");
        let entry = node.entry("big").await.unwrap();
        assert_eq!(entry.state, EntryState::Evicted, "nothing is cached");
    });
}

#[test]
fn a_version_that_is_not_evicted_is_not_filled() {
    runtime().block_on(async {
        let node = Node::open(11).await;
        let store = remote(11, false);
        let filler = filler(&store, 4, &Counters::default());
        let seq = node.put("k", "dirty").await;
        let version = node.entry("k").await.unwrap().version;
        assert_eq!(version.seq.get(), seq);
        let requests = store.stats().requests;
        assert_eq!(
            read(&filler, &node, "k", version, 0..5).await,
            Err(FillError::Changed)
        );
        assert_eq!(
            read(&filler, &node, "missing", version, 0..5).await,
            Err(FillError::Changed)
        );
        assert_eq!(store.stats().requests, requests, "nothing was read");
    });
}

#[test]
fn a_remote_object_of_another_size_fails_the_fill() {
    runtime().block_on(async {
        let node = Node::open(12).await;
        let store = remote(12, false);
        let filler = filler(&store, 4, &Counters::default());
        let etag = write_remote(&store, "k", "twelve bytes").await;
        // The stub says the object is shorter than it is.
        let stub = import(&node, "k", 5, etag).await;
        let failed = read(&filler, &node, "k", stub, 0..5).await;
        assert!(
            matches!(&failed, Err(FillError::Failed(reason)) if reason.contains("12-byte")),
            "{failed:?}"
        );
    });
}
